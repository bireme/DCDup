/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.{BufferedWriter, File}
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.Calendar

import br.bireme.ngrams.{NGIndex, NGSchema, NGrams}
import org.bireme.dcdup.akka.CheckDuplicatedAkka

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

/** Checks a DeDup input piped file against a Dedup index looking for duplicated docs.
  *
  * author: Heitor Barbieri
  * date: 20190329
*/
object CheckDuplicated {
  /** Checks a DeDup input piped file against a Dedup index looking for duplicated docs.
  *
    * @param pipeFile the input piped file with documents that will be checked
    * @param pipeFileEncod the input piped file's character encoding
    * @param luceneIndex if it is to check against an index then the index path otherwise (None) check against itself
    * @param ngSchema DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param outDupFile the name of the output no duplicated documents file
    * @param outNoDupFile the output file's character encoding
    * @param selfCheck if true indicate that it is a duplicated check from input file against itself
    *                  (the index contains only the input file)
    */
  def checkDuplicated(pipeFile: String,
                      pipeFileEncod: String,
                      luceneIndex: Option[String],
                      ngSchema: NGSchema,
                      outDupFile: String,
                      outNoDupFile: String,
                      selfCheck: Boolean = false): Unit = {

    val time: String = Calendar.getInstance().getTimeInMillis.toString
    val indexPath: String = luceneIndex.getOrElse {
      print("Creating temporary index ... ")
      val tmpIndexPath: String = createTmpIndex(pipeFile, pipeFileEncod, ngSchema, time)
      println("OK")
      tmpIndexPath
    }
    val index: NGIndex = new NGIndex(indexPath, indexPath, true)

    // Check against the index (self or remote)
    println("Looking for duplicated documents in piped file... ")
    check(index, ngSchema, pipeFile, pipeFileEncod, outDupFile + "_tmp", selfCheck)

    index.close()
    println("... OK")

    // Retrieve a map of repeated ids.
    print("Post processing duplicated files ... ")
    val dupIds: Map[String, Set[String]] = postProcessDup(outDupFile + "_tmp", outDupFile)

    // Remove repeatable documents
    print("OK\nPost processing no duplicated files... ")
    val idsDup: Set[String] = dupIds.foldLeft(Set[String]()) ((set, kv) => set ++ (kv._2 + kv._1))
    postProcessNoDup(pipeFile, pipeFileEncod, ngSchema, outNoDupFile, idsDup)
    println("OK")

    // Delete temporary files
    if (luceneIndex.isEmpty) deleteFile(new File(indexPath))
    deleteFile(new File(outDupFile + "_tmp"))
  }

  /** Creates a pipe file that contains the lines from both remote file and those that are not repeated from self file
    *
    * @param ngSchema DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param selfNoDupFile the self check no duplicated file
    * @param remoteNoDupFile the remote check no duplicated file
    * @param outNoDupFile the unified no duplicated file
    */
  def takeNoDuplicatedLight(ngSchema: NGSchema,
                            selfNoDupFile: String,
                            remoteNoDupFile: String,
                            outNoDupFile: String): Unit = {
    val idPos: Integer = ngSchema.getNamesPos.get("id")
    val dbPos: Integer = ngSchema.getNamesPos.get("database")
    val elemNum: Int = ngSchema.getNamesPos.size
    val out: BufferedWriter = Files.newBufferedWriter(Paths.get(outNoDupFile), Charset.forName("utf-8"))

    def writeToFile(outFile: String,
                    ids: Set[String]): Set[String] = {
      val in: BufferedSource = Source.fromFile(outFile, "utf-8")
      val outSet: Set[String] = in.getLines().foldLeft(ids) {
        case (set, line) =>
          val lineT = line.trim
          if (lineT.isEmpty) set
          else {
            val id: String = getId(idPos, dbPos, elemNum, lineT)  //iddb
            if (set.contains(id)) set
            else {
              out.write(line + "\n")
              set + id
            }
          }
      }
      in.close()
      outSet
    }

    val ids1: Set[String] = writeToFile(remoteNoDupFile, Set[String]())
    writeToFile(selfNoDupFile, ids1)
    out.close()
  }

  /** Creates a pipe file that contains the lines from both nodup files but without repeating lines.
    *
    * @param ngSchema DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param selfNoDupFile the self check no duplicated file
    * @param remoteNoDupFile the remote check no duplicated file
    * @param outNoDupFile the unified no duplicated file
    */
  def takeNoDuplicated(ngSchema: NGSchema,
                               selfNoDupFile: String,
                               remoteNoDupFile: String,
                               outNoDupFile: String): Unit = {
    val idPos: Integer = ngSchema.getNamesPos.get("id")
    val dbPos: Integer = ngSchema.getNamesPos.get("database")
    val elemNum: Int = ngSchema.getNamesPos.size
    val out: BufferedWriter = Files.newBufferedWriter(Paths.get(outNoDupFile), Charset.forName("utf-8"))

    // Take ids from self no duplicated file
    val in1: BufferedSource = Source.fromFile(selfNoDupFile, "utf-8")
    val ids1: Set[String] = in1.getLines().foldLeft(Set[String]()) {
      case (set, line) =>
        val lineT: String = line.trim
        if (lineT.isEmpty) set
        else set + getId(idPos, dbPos, elemNum, lineT) //iddb
    }
    in1.close()

    // Take ids from remote no duplicated file
    val in2: BufferedSource = Source.fromFile(remoteNoDupFile, "utf-8")
    val ids2: Set[String] = in2.getLines().foldLeft(Set[String]()) {
      case (set, line) =>
        val lineT: String = line.trim
        if (lineT.isEmpty) set
        else set + getId(idPos, dbPos, elemNum, lineT) //iddb
    }
    in2.close()

    // General no repeatable ids
    val nrepIds: Set[String] = ids1 intersect ids2

    // Write no repeatable docs from self file
    val in1a: BufferedSource = Source.fromFile(selfNoDupFile, "utf-8")
    in1a.getLines().foreach {
      line =>
        val lineT: String = line.trim
        val id: String = getId(idPos, dbPos, elemNum, lineT) //iddb
        if (nrepIds.contains(id)) out.write(lineT + "\n")
    }
    in1a.close()

    // Write no repeatable docs from remote file
    val in2a: BufferedSource = Source.fromFile(remoteNoDupFile, "utf-8")
    in2a.getLines().foreach {
      line =>
        val lineT: String = line.trim
        val id: String = getId(idPos, dbPos, elemNum, lineT) //iddb
        if (nrepIds.contains(id) && !ids1.contains(id)) out.write(lineT + "\n")
    }
    in2a.close()

    out.close()
  }

  /** Deletes a standard file or a directory recursively
    *
    * @param file file or directory to be deleted
    */
  def deleteFile(file: File): Unit = {
    val contents: Array[File] = file.listFiles()
    if (contents != null) contents.foreach(deleteFile)
    file.delete()
  }

  /**
    * Given an input piped file and a NGrams index, looks for documents that
    * are in the input file that are similar to the ones stored in the index.
    *
    * @param ngIndex NGrams index path
    * @param ngSchema NGrams index/search configuration file
    * @param pipeFile input piped file used to look for similar docs
    * @param pipeFileEncoding input piped file character encoding
    * @param outDupFile output piped file with the similar documents
    * @param selfCheck if true indicates that are a duplicated check from input file against itself
    */
  private def check(ngIndex: NGIndex,
                    ngSchema: NGSchema,
                    pipeFile: String,
                    pipeFileEncoding: String,
                    outDupFile: String,
                    selfCheck: Boolean): Unit = {
    //NGrams.search(ngIndex, ngSchema, pipeFile, pipeFileEncoding, outDupFile, "utf-8")

    val numberOfCheckers: Int = Runtime.getRuntime.availableProcessors()
    CheckDuplicatedAkka.check(ngIndex, ngSchema, pipeFile, pipeFileEncoding, outDupFile,
      numberOfCheckers = numberOfCheckers, selfCheck)
  }

  /** Creates a temporary DeDup index.
    *
    * @param pipeFile piped file with documents that will populate the index
    * @param pipeFileEncoding piped file character encoding
    * @param ngSchema DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param time time string used as a suffix of the index name
    * @return the index's path
    */
  private def createTmpIndex(pipeFile: String,
                             pipeFileEncoding: String,
                             ngSchema: NGSchema,
                             time: String): String = {
    val indexPath: String = s"/tmp/DCDup_$time"
    val ngIndex: NGIndex = new NGIndex(indexPath, indexPath, false)

    NGrams.index(ngIndex, ngSchema, pipeFile, pipeFileEncoding)
    ngIndex.close()

    indexPath
  }

  /** Create an output file where the duplicated ids from input file are removed
    * @param outDupFileIn output file to remove duplicates
    * @param outDupFileOut output file with duplicates removed after post processing
    * @return the map of repetitive ids: (id -> (id, id, ..., id))
    */
  def postProcessDup(outDupFileIn: String,
                     outDupFileOut: String): Map[String, Set[String]] = {
    val ids: mutable.Map[String, Set[String]] = mutable.Map[String, Set[String]]()
    val out: BufferedWriter = Files.newBufferedWriter(Paths.get(outDupFileOut), Charset.forName("utf-8"))
    val in: BufferedSource = Source.fromFile(outDupFileIn, "utf-8")

    in.getLines() foreach {
      line =>
        val linet: String = line.trim
        if (linet.nonEmpty) {
          val split: Array[String] = linet.split(" *\\| *", 8)  // ranking|similarity|id1|id2|tit1|tit2|db1|db2
          if (split.length == 8) {
            val id1x: String = getId(2, 6,8, linet) //id1db1
            val id2x: String = getId(3, 7,8, linet) //id2db2
            val (id1, id2) = if (id1x.compareTo(id2x) <= 0) (id1x, id2x) else (id2x, id1x)

            if (!id1.equals(id2)) {
              val set: Set[String] = ids.getOrElse(id1, Set[String]())
              if (!set.contains(id2)) {
                out.write(line + "\n")
                ids += (id1 -> (set + id2))
                //println(s"id1=$id1 id2=$id2 - ++++ DENTRO!")
              }
            }
          } else println("skipping line: " + linet)
        }
    }
    in.close()
    out.close()
    ids.toMap
  }

  /**
  * Given the input piped file and the duplicated file, create the no duplicated file by removing from the first, the
    * lines that are not identified in the second one.
    * @param pipeFile the input piped file with documents that will be checked
    * @param pipeFileEncoding the input piped file's character encoding
    * @param ngSchema DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param outNoDupFile the name of the output no duplicated documents file
    * @param dpIds the ids of all duplicated documents
    */
  def postProcessNoDup(pipeFile: String,
                       pipeFileEncoding: String,
                       ngSchema: NGSchema,
                       outNoDupFile: String,
                       dpIds: Set[String]): Unit = {
    val idPos: Integer = ngSchema.getNamesPos.get("id")
    val dbPos: Integer = ngSchema.getNamesPos.get("database")
    val elemNum: Int = ngSchema.getNamesPos.size()
    val out: BufferedWriter = Files.newBufferedWriter(Paths.get(outNoDupFile), Charset.forName("utf-8"))
    val idsNoDup: mutable.Set[String] = mutable.Set[String]()

    val inPipe: BufferedSource = Source.fromFile(pipeFile, pipeFileEncoding)
    inPipe.getLines().foreach {
      line =>
        val linet: String = line.trim
        if (linet.nonEmpty) {
          val id: String = getId(idPos, dbPos, elemNum, linet) //iddb
          if (!dpIds.contains(id) && !idsNoDup.contains(id)) {
            idsNoDup += id
            out.write(line + "\n")
          }
        }
    }
    inPipe.close()
    out.close()
  }

  /** Get the id from a pipe line and compose a new one by joining it with the database name (iddb)
    *
    * @param idPos position of the piped line where the elemente 'id' is
    * @param dbPos position of the piped line where the database 'element' is
    * @param elemNum number of elements of the piped line
    * @param line input piped line
    * @return the new id = id + databasename
    */
  private def getId(idPos: Int,
                    dbPos: Int,
                    elemNum: Int,
                    line: String): String = {
    val split: Array[String] = line.split(" *\\| *", elemNum + 1)
    val id: String = split(idPos)
    val posHifen: Int = id.indexOf('^')   // usually id^language exported by MySQL2Pipe
    val pos: Int = if (posHifen == -1) id.length else posHifen

    Tools.normalize(id.substring(0, pos) + split(dbPos))   //iddb
  }
}
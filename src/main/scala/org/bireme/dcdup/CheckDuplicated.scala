/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup
import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.Calendar

import br.bireme.ngrams.{NGIndex, NGSchema, NGrams}

import scala.collection.mutable
import scala.io.Source

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
    * @param outDupEncod the duplicated and no duplicated files' character encoding
    */
  def checkDuplicated(pipeFile: String,
                      pipeFileEncod: String,
                      luceneIndex: Option[String],
                      ngSchema: NGSchema,
                      outDupFile: String,
                      outNoDupFile: String,
                      outDupEncod: String = "utf-8") {

    val time = Calendar.getInstance().getTimeInMillis.toString
    val indexPath = luceneIndex.getOrElse {
      print("Creating temporary index ... ")
      val tmpIndexPath = createTmpIndex(pipeFile, pipeFileEncod, ngSchema, time)
      println("OK")
      tmpIndexPath
    }
    val index = new NGIndex(indexPath, indexPath, true)

    // Self check
    print("Looking for duplicated documents in piped file ... ")
    check(index, ngSchema, pipeFile, pipeFileEncod, outDupFile + "_tmp", outDupEncod)
    println("OK")

    println("Post processing duplicated files ... ")
    val dupIds: Map[String, Set[String]] = postProcessDup(outDupFile + "_tmp", outDupFile, outDupEncod)
    println("OK\nPost processing no duplicated files ... ")
    val idsDup: Set[String] = dupIds.foldLeft(Set[String]()) ((set, kv) => set ++ (kv._2 + kv._1))
    postProcessNoDup(pipeFile, pipeFileEncod, ngSchema, outNoDupFile, outDupEncod, idsDup)
    println("OK")

    // Delete temporary files
    if (luceneIndex.isEmpty) deleteFile(new File(indexPath))
    deleteFile(new File(outDupFile + "_tmp"))
  }

  /** Creates a pipe file that contains the lines from both nodup files but without repeating lines.
    *
    * @param ngSchema DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param outNoDupFile1 the self check no duplicated file
    * @param outNoDupFile2 the remote check no duplicated file
    * @param outNoDupFile the unified no duplicated file
    * @param outDupFileEncoding the files's character encoding
    */
  def takeNoDuplicated(ngSchema: NGSchema,
                       outNoDupFile1: String,
                       outNoDupFile2: String,
                       outNoDupFile: String,
                       outDupFileEncoding: String): Unit = {
    val idPos = ngSchema.getNamesPos.get("id")
    val dbPos = ngSchema.getNamesPos.get("database")
    val elemNum = ngSchema.getNamesPos.size
    val out = Files.newBufferedWriter(Paths.get(outNoDupFile), Charset.forName(outDupFileEncoding))
    val in1 = Source.fromFile(outNoDupFile1, outDupFileEncoding)
    val ids = in1.getLines().foldLeft(Set[String]()) {
      case (set, line) =>
        val lineT = line.trim
        if (lineT.isEmpty) set
        else {
          val id = getId(idPos, dbPos, elemNum, lineT)
          if (set.contains(id)) set
          else {
            out.write(line + "\n")
            set + id
          }
        }
    }
    in1.close()

    val in2 = Source.fromFile(outNoDupFile2, outDupFileEncoding)
    in2.getLines().foreach {
      line =>
        val lineT = line.trim
        if (lineT.nonEmpty) {
          val id = getId(idPos, dbPos, elemNum, lineT)
          if (!ids.contains(id)) out.write(line + "\n")
        }
    }
    in2.close()
    out.close()
  }

  /** Deletes a standard file or a directory recursivelly
    *
    * @param file file or directory to be deleted
    */
  def deleteFile(file: File): Unit = {
    val contents = file.listFiles()
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
    * @param outDupFileEncoding output piped file character encoding
    */
  private def check(ngIndex: NGIndex,
                    ngSchema: NGSchema,
                    pipeFile: String,
                    pipeFileEncoding: String,
                    outDupFile: String,
                    outDupFileEncoding: String): Unit = {
    NGrams.search(ngIndex, ngSchema, pipeFile, pipeFileEncoding, outDupFile, outDupFileEncoding)
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
    val indexPath = s"/tmp/DCDup_$time"
    val ngIndex = new NGIndex(indexPath, indexPath, false)

    NGrams.index(ngIndex, ngSchema, pipeFile, pipeFileEncoding)
    indexPath
  }

  /**
    * Avoid duplicated entries in the output duplicated file (the check of input piped file with itself.
    * Comparation of document with itself are removed from the new output duplicated file
    * @param outDupFileIn output file to remove duplicates
    * @param outDupFileOut output file with duplicates removed after post processing
    * @param outDupEncod output file encoding
    * @return the map of repetitive ids: (id -> (id, id, ..., id))
    */
  def postProcessDup(outDupFileIn: String,
                     outDupFileOut: String,
                     outDupEncod: String): Map[String, Set[String]] = {
    val ids = mutable.Map[String, Set[String]]()
    val out = Files.newBufferedWriter(Paths.get(outDupFileOut), Charset.forName(outDupEncod))
    val in = Source.fromFile(outDupFileIn, outDupEncod)

    in.getLines() foreach {
      line =>
        val linet = line.trim
        if (linet.nonEmpty) {
          val split = linet.split(" *\\| *", 8)  // ranking|similarity|id1|id2|tit1|tit2|db1|db2
          val pos1x = split(2).indexOf('-')
          val pos2x = split(3).indexOf('-')
          val pos1 = if (pos1x == -1) split(2).length else pos1x
          val pos2 = if (pos2x == -1) split(3).length else pos2x
          val id1x = Tools.normalize(split(2).substring(0, pos1) + split(6))   //id1db1
          val id2x = Tools.normalize(split(3).substring(0, pos2) + split(7))   //id2db2
          val (id1, id2) = if (id1x.compareTo(id2x) <= 0) (id1x,id2x) else (id2x,id1x)

          if (!id1.equals(id2)) {
            val set = ids.getOrElse(id1, Set[String]())
            if (!set.contains(id2)) {
              out.write(line + "\n")
              ids += (id1 -> (set + id2))
              //println(s"id1=$id1 id2=$id2 - ++++ DENTRO!")
            }
          }
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
    * @param outNoDupEncod the output file's character encoding
    * @param dpIds the ids of all duplicated documents
    */
  def postProcessNoDup(pipeFile: String,
                       pipeFileEncoding: String,
                       ngSchema: NGSchema,
                       outNoDupFile: String,
                       outNoDupEncod: String,
                       dpIds: Set[String]): Unit = {
    val idPos = ngSchema.getNamesPos.get("id")
    val dbPos = ngSchema.getNamesPos.get("database")
    val elemNum = ngSchema.getNamesPos.size()
    val out = Files.newBufferedWriter(Paths.get(outNoDupFile), Charset.forName(outNoDupEncod))
    val idsNoDup: mutable.Set[String] = mutable.Set[String]()

    val inPipe = Source.fromFile(pipeFile, pipeFileEncoding)
    inPipe.getLines().foreach {
      line =>
        val linet = line.trim
        if (linet.nonEmpty) {
          val split = linet.split(" *\\| *", elemNum)
          val idx: String = split(idPos)
          val posx = idx.indexOf('-')
          val pos = if (posx == -1) idx.length else posx
          val id = Tools.normalize(idx.substring(0, pos) + split(dbPos)) //iddb

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
    val split = line.split(" *\\| *", elemNum)
    val idx = split(idPos)
    val posx = idx.indexOf('-')
    val pos = if (posx == -1) idx.length else posx

    Tools.normalize(split(2).substring(0, pos) + split(dbPos))   //iddb
  }
}
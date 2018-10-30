/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import br.bireme.ngrams.{NGrams,NGIndex,NGSchema}

import java.io.{File,IOException}
import java.nio.file.{Files,Paths}
import java.nio.charset.{Charset, CodingErrorAction}
import java.util.Calendar

import scala.io.{Codec, Source}

/** Checks a DeDup input piped file against itself to look for duplicated
  * documents and then against a Dedup index (usually LILACS) to also
  * look for duplicated docs.
  *
  * author: Heitor Barbieri
  * date: 20161208
*/
object DoubleCheckDuplicated extends App {
  private def usage(): Unit = {
    System.err.println("usage: DoubleCheckDuplicated " +
      "\n\t<pipeFile> - DeDup piped input file" +
      "\n\t<pipeFileEncoding> - pipe file character encoding" +
      "\n\t<luceneIndex> - path to Dedup's Lucene index" +
      "\n\t<confFile> - DeDup fields configuration file" +
      "\n\t<confFileEncoding> - DeDup fields configuration file character encoding" +
      "\n\t<outDupFile1> - duplicated records found in pipe file" +
      "\n\t<outDupFile2> - duplicated records found between pipe file and Dedup index" +
      "\n\t<outNoDupFile> - no duplicated records between (pipe file and itself) " +
      "and (pipe file and Dedup index)" +
      "\n\t[<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 8) usage()
  val outEncoding = if (args.length > 8) args(8) else "ISO-8859-1"

  doubleCheck(args(0), args(1), args(2), args(3), args(4), args(5),
              args(6), args(7), outEncoding)

  def doubleCheck(pipeFile: String,
                  pipeFileEncoding: String,
                  luceneIndex: String,
                  confFile: String,
                  confFileEncoding: String,
                  outDupFile1: String,
                  outDupFile2: String,
                  outNoDupFile: String,
                  outDupFileEncoding: String): Unit = {
    val time = Calendar.getInstance().getTimeInMillis().toString
    val ngSchema = new NGSchema(confFile, confFile, confFileEncoding)
    val tmpIndexPath = createTmpIndex(pipeFile, pipeFileEncoding, ngSchema, time)
    val tmpIndex = new NGIndex(tmpIndexPath, tmpIndexPath, true)
    val ngIndex = new NGIndex(luceneIndex, luceneIndex, true)

    // Self check
    check(tmpIndex, ngSchema, pipeFile, pipeFileEncoding, outDupFile1,
                                                             outDupFileEncoding)
    // Check using given Lucene indexPath
    check(ngIndex, ngSchema, pipeFile, pipeFileEncoding, outDupFile2,
                                                             outDupFileEncoding)
    // Take duplicate no duplicated documents
    takeNoDuplicated(pipeFile, pipeFileEncoding, outDupFile1, outDupFile2,
                     outNoDupFile, outDupFileEncoding)
    // Delete temporary files
    deleteFile(new File(tmpIndexPath))
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
    NGrams.search(ngIndex, ngSchema, pipeFile, pipeFileEncoding,
                  outDupFile, outDupFileEncoding)
  }

  /** Creates a temporary DeDup index.
    *
    * @param pipeFile piped file with documents that will populate the index
    * @param pipeFileEncoding piped file character encoding
    * @param ngSchema DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param time time string used as a suffix of the index name
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

  /** Creates a pipe file that is equal to the first one less the lines whose
   * identifiers are also in the second and third ones.
   *
   * @param pipeFile the first piped file (the original input)
   * @param pipeFileEncoding the first piped file character encoding
   * @param outDupFile1 the second piped file (checked with itself)
   * @param outDupFile2 the third piped file  (checked with DeDup index)
   * @param outNoDupFile the ouput piped file with no duplicated records
   * @param outDupFileEncoding the no duplicated record file character encoding
   */
  private def takeNoDuplicated(pipeFile: String,
                               pipeFileEncoding: String,
                               outDupFile1: String,
                               outDupFile2: String,
                               outNoDupFile: String,
                               outDupFileEncoding: String): Unit = {
    val auxIds = getIds(outDupFile1, outDupFileEncoding)
    val ids = auxIds ++ getIds(outDupFile2, outDupFileEncoding, onlyFirstId= true)
    val codec = pipeFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val in = Source.fromFile(pipeFile)(decoder)
    val out = Files.newBufferedWriter(Paths.get(outNoDupFile),
                                      Charset.forName(pipeFileEncoding))
    var first = true

    in.getLines().foreach {
      line => getIdFromLine(line).foreach {
        id =>
          if (!ids.contains(id)) {
            if (first) first = false else out.write("\n")
            out.write(line)
          }
      }
    }
    in.close()
    out.close()
  }

  /** Given an output piped file resulting from NGrams check and returns all
    * ids that identifies duplicated documents (if onlyFirstId is false) or
    * the lower one from the duple (if onlyFirstId is true)
    * @param outDupFile duplicated doc piped file
    * @param outDupFileEncoding duplicated doc piped file encoding
    * @param onlyFirstId if true returns only the first id from both of duplicated
    *                   docs otherwise it retuns both ids
    * @return a set of duplicated doc ids
    */
  private def getIds(outDupFile: String,
                     outDupFileEncoding: String,
                     onlyFirstId: Boolean = false): Set[String] = {
    val codec = outDupFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val reader = Source.fromFile(outDupFile)(decoder)
    val in = reader.getLines()

    // Skip two first header licenses
    in.drop(2)

    val outSet = in.foldLeft[Set[String]](Set()) {
      case (set, line) => getSimIdsFromLine(line) match {
        case Some((id1,id2)) =>
          if (onlyFirstId) set + id1
          else set + (id1, id2)
        case None => set
      }
    }
    reader.close()
    outSet
  }

  /** Given a NGrams output line (piped), retrives the two similar doc ids.
    *
    * @param line ids piped line
    */
  private def getSimIdsFromLine(line: String): Option[(String,String)] = {
    val linet = line.trim()

    if (linet.isEmpty) None
    else {
      val split = linet.split("\\|", 5)
      if (split.length != 5) throw new IOException(linet)
      Some((split(2), split(3)))
    }
  }

  /** Given a NGrams input line (piped), retrives the doc id.
    *
    * @param line ids piped line
    */
  private def getIdFromLine(line: String): Option[String] = {
    val linet = line.trim()

    if (linet.isEmpty) None
    else {
      val split = linet.split("\\|", 3)
      if (split.length != 3) throw new IOException(linet)
      Some(split(1))
    }
  }

  /** Deletes a standard file or a directory recursivelly
    *
    * @param file file or directory to be deleted
    */
  private def deleteFile(file: File): Unit = {
    val contents = file.listFiles()
    if (contents != null) contents.foreach(deleteFile(_))
    file.delete()
  }
}

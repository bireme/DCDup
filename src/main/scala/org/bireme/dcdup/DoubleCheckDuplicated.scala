/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.File
import br.bireme.ngrams.NGSchema

/** Check a DeDup input piped file against itself to look for duplicated
  * documents and then against a DeDup index (usually LILACS) to also
  * look for duplicated ones.
  *
  * author: Heitor Barbieri
  * date: 20161208
*/
object DoubleCheckDuplicated extends App {
  private def usage(): Unit = {
    System.err.println("usage: DoubleCheckDuplicated " +
      "\n\t-pipeFile=<pipeFile> - DeDup piped input file" +
      "\n\t-index=<luceneIndex> - path to Dedup's Lucene index" +
      "\n\t-confFile=<confFile> - DeDup fields configuration file (schema)" +
      "\n\t-outDupFile1=<outDupFile1> - duplicated records found in pipe file" +
      "\n\t-outDupFile2=<outDupFile2> - duplicated records found between pipe file and Dedup index" +
      "\n\t-outNoDupFile=<outNoDupFile> - no duplicated records between (pipe file and itself) and (pipe file and Dedup index)" +
      "\n\t[-pipeFileEncod=<pipeFileEncoding>] - pipe file character encoding. Default is utf-8" +
      "\n\t[-confFileEncod=<confFileEncoding>] - DeDup fields configuration file character encoding" +
      "\n\t[-outDupEncod=<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 6) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split: Array[String] = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  val pipeFile: String = parameters("pipeFile")
  val index: String = parameters("index")
  val confFile: String = parameters("confFile")
  val outDupFile1: String = parameters("outDupFile1")
  val outDupFile2: String = parameters("outDupFile2")
  val outNoDupFile: String = parameters("outNoDupFile")
  val pipeFileEncod: String = parameters.getOrElse("pipeFileEncod", "utf-8")
  val confFileEncod: String = parameters.getOrElse("confFileEncod", "utf-8")
  val outDupEncod: String = parameters.getOrElse("outDupEncod", "utf-8")

  doubleCheck(pipeFile, pipeFileEncod, index, confFile, confFileEncod,
              outDupFile1, outDupFile2, outNoDupFile,outDupEncod)


  /** Check a DeDup input piped file against itself to look for duplicated
    * documents and then against a DeDup index (usually LILACS) to also
    * look for duplicated ones.
    *
    * @param pipeFile DeDup piped input file
    * @param pipeFileEncoding pipe file character encoding
    * @param luceneIndex path to Dedup's Lucene index
    * @param confFile DeDup fields configuration file (schema)
    * @param confFileEncoding DeDup fields configuration file character encoding
    * @param outDupFile1 duplicated records found in pipe file
    * @param outDupFile2  duplicated records found between pipe file and Dedup index
    * @param outNoDupFile no duplicated records between (pipe file and itself) and (pipe file and Dedup index)
    * @param outDupFileEncoding output file character encoding.
    */
  def doubleCheck(pipeFile: String,
                  pipeFileEncoding: String,
                  luceneIndex: String,
                  confFile: String,
                  confFileEncoding: String,
                  outDupFile1: String,
                  outDupFile2: String,
                  outNoDupFile: String,
                  outDupFileEncoding: String): Unit = {
    val ngSchema: NGSchema = new NGSchema(confFile, confFile, confFileEncoding)

    // Self check
    CheckDuplicated.checkDuplicated(pipeFile, pipeFileEncoding , None, ngSchema, outDupFile1,
      outNoDupFile + "_self", outDupFileEncoding)

    // Check using given Lucene indexPath
    CheckDuplicated.checkDuplicated(pipeFile, pipeFileEncoding , Some(luceneIndex), ngSchema, outDupFile2,
      outNoDupFile + "_remote", outDupFileEncoding)

    // Take duplicate no duplicated documents
    CheckDuplicated.takeNoDuplicated(ngSchema, outNoDupFile + "_self", outNoDupFile + "_remote",
      outNoDupFile, outDupFileEncoding)

    // Delete pre-processed output files
    CheckDuplicated.deleteFile(new File(outNoDupFile + "_self"))
    CheckDuplicated.deleteFile(new File(outNoDupFile + "_remote"))
  }
}

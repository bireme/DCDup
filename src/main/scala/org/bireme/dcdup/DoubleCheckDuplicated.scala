/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.File
import java.util.Calendar

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
      "\n\t-outNoDupFile1=<outNoDupFile1> - no duplicated records between input pipe file and Dedup index" +
      "\n\t-outNoDupFile2=<outNoDupFile2> - no duplicated records between (pipe file and itself) and (pipe file and Dedup index)" +
      "\n\t[-pipeFileEncod=<pipeFileEncoding>] - pipe file character encoding. Default is utf-8" +
      "\n\t[-confFileEncod=<confFileEncoding>] - DeDup fields configuration file character encoding")
    System.exit(1)
  }

  if (args.length < 7) usage()

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
  val outNoDupFile1: String = parameters("outNoDupFile1")
  val outNoDupFile2: String = parameters("outNoDupFile2")
  val pipeFileEncod: String = parameters.getOrElse("pipeFileEncod", "utf-8")
  val confFileEncod: String = parameters.getOrElse("confFileEncod", "utf-8")
  val begin: Long = Calendar.getInstance.getTimeInMillis

  doubleCheck(pipeFile, pipeFileEncod, index, confFile, confFileEncod,
              outDupFile1, outDupFile2, outNoDupFile1, outNoDupFile2)

  println(s"\nElapsed time: ${(Calendar.getInstance.getTimeInMillis - begin) / 1000}s")

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
    * @param outNoDupFile1 no duplicated records between input pipe file and Dedup index
    * @param outNoDupFile2 no duplicated records between (pipe file and itself) and (pipe file and Dedup index)
    */
  def doubleCheck(pipeFile: String,
                  pipeFileEncoding: String,
                  luceneIndex: String,
                  confFile: String,
                  confFileEncoding: String,
                  outDupFile1: String,
                  outDupFile2: String,
                  outNoDupFile1: String,
                  outNoDupFile2: String): Unit = {
    val ngSchema: NGSchema = new NGSchema(confFile, confFile, confFileEncoding)

    // Self check
    CheckDuplicated.checkDuplicated(pipeFile, pipeFileEncoding , None, ngSchema, outDupFile1, outNoDupFile1 + "_self",
      selfCheck = true)

    // Check using given Lucene indexPath
    CheckDuplicated.checkDuplicated(pipeFile, pipeFileEncoding , Some(luceneIndex), ngSchema, outDupFile2,
      outNoDupFile1, selfCheck = false)

    // Take duplicate no duplicated documents between (pipe file and itself) and (pipe file and Dedup index)
    CheckDuplicated.takeNoDuplicated(ngSchema, outNoDupFile1 + "_self" , outNoDupFile1, outNoDupFile2)

    // Delete pre-processed output file
    CheckDuplicated.deleteFile(new File(outNoDupFile1 + "_self"))
  }
}

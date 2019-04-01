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
      "\n\t<pipeFile> - DeDup piped input file" +
      "\n\t<pipeFileEncoding> - pipe file character encoding" +
      "\n\t<luceneIndex> - path to Dedup's Lucene index" +
      "\n\t<confFile> - DeDup fields configuration file (schema)" +
      "\n\t<confFileEncoding> - DeDup fields configuration file character encoding" +
      "\n\t<outDupFile1> - duplicated records found in pipe file" +
      "\n\t<outDupFile2> - duplicated records found between pipe file and Dedup index" +
      "\n\t<outNoDupFile> - no duplicated records between (pipe file and itself) and (pipe file and Dedup index)" +
      "\n\t[<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 8) usage()
  val outEncoding = if (args.length > 8) args(8) else "utf-8"

  doubleCheck(args(0), args(1), args(2), args(3), args(4), args(5),
              args(6), args(7), outEncoding)

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
    CheckDuplicated.checkDuplicated(pipeFile, pipeFileEncoding , Some(luceneIndex), ngSchema, outDupFile1,
      outNoDupFile + "_remote", outDupFileEncoding)

    // Take duplicate no duplicated documents
    CheckDuplicated.takeNoDuplicated(ngSchema, outDupFile1, outDupFile2, outNoDupFile, outDupFileEncoding)

    // Delete pre-processed output files
    CheckDuplicated.deleteFile(new File(outNoDupFile + "_self"))
    CheckDuplicated.deleteFile(new File(outNoDupFile + "_remote"))
  }
}

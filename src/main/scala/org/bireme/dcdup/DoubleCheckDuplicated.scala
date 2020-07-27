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
      "\n\t-pipe=<pipeFile> - DeDup piped input file" +
      "\n\t-index=<indexPath> - DeDup index path" +
      "\n\t-schema=<schemaFile> - DeDup schema file" +
      "\n\t-outDupFile1=<outDupFile1> - duplicated records found in pipe file" +
      "\n\t-outDupFile2=<outDupFile2> - duplicated records found between pipe file and DeDup index" +
      "\n\t-outNoDupFile1=<outNoDupFile1> - no duplicated records between input pipe file and Dedup index" +
      "\n\t-outNoDupFile2=<outNoDupFile2> - no duplicated records between (pipe file and itself) and (pipe file and Dedup index)" +
      "\n\t[-schemaEncoding=<schemaFileEncoding>] - NGram schema file encoding. Default is utf-8" +
      "\n\t[-pipeEncoding=<pipeEncoding>] - pipe file encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 7) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split: Array[String] = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }
  val keys = parameters.keys.toSet
  if (!Set("pipe", "index", "schema", "outDupFile1", "outDupFile2", "outNoDupFile1", "outNoDupFile2")
    .forall(keys.contains)) usage()

  val pipe: String = parameters("pipe")
  val index: String = parameters("index")
  val schema: String = parameters("schema")
  val outDupFile1: String = parameters("outDupFile1")
  val outDupFile2: String = parameters("outDupFile2")
  val outNoDupFile1: String = parameters("outNoDupFile1")
  val outNoDupFile2: String = parameters("outNoDupFile2")
  val pipeEncoding: String = parameters.getOrElse("pipeEncoding", "utf-8")
  val schemaEncoding: String = parameters.getOrElse("schemaEncoding", "utf-8")
  val begin: Long = Calendar.getInstance.getTimeInMillis

  doubleCheck(pipe, pipeEncoding, index, schema, schemaEncoding,
              outDupFile1, outDupFile2, outNoDupFile1, outNoDupFile2)

  println(s"\nElapsed time: ${(Calendar.getInstance.getTimeInMillis - begin) / 1000}s")

  /** Check a DeDup input piped file against itself to look for duplicated
    * documents and then against a DeDup index (usually LILACS) to also
    * look for duplicated ones.
    *
    * @param pipe DeDup piped input file
    * @param pipeEncoding pipe file character encoding
    * @param luceneIndex path to Dedup's Lucene index
    * @param schema DeDup fields configuration file (schema)
    * @param schemaEncoding DeDup fields configuration file character encoding
    * @param outDupFile1 duplicated records found in pipe file
    * @param outDupFile2  duplicated records found between pipe file and Dedupa index
    * @param outNoDupFile1 no duplicated records between input pipe file and Dedup index
    * @param outNoDupFile2 no duplicated records between (pipe file and itself) and (pipe file and Dedup index)
    */
  def doubleCheck(pipe: String,
                  pipeEncoding: String,
                  luceneIndex: String,
                  schema: String,
                  schemaEncoding: String,
                  outDupFile1: String,
                  outDupFile2: String,
                  outNoDupFile1: String,
                  outNoDupFile2: String): Unit = {
    val ngSchema: NGSchema = new NGSchema(schema, schema, schemaEncoding)

    // Verifying pipe file integrity
    println("\nVerifying pipe file integrity")
    val goodFileName = File.createTempFile("good", "").getPath
    val badFileName = File.createTempFile("bad", "").getPath
    val (good,bad) = VerifyPipeFile.checkLocal(pipe, pipeEncoding,
      schema, goodFileName, badFileName, schemaEncoding)
    println(s"Using $good documents")
    if (bad > 0) println(s"Skipping $bad documents. See file: $badFileName\n")

    // Self check
    CheckDuplicated.checkDuplicated(goodFileName, pipeEncoding , None, ngSchema, outDupFile1, outNoDupFile1 + "_self",
      selfCheck = true)

    // Check using given Lucene indexPath
    CheckDuplicated.checkDuplicated(goodFileName, pipeEncoding , Some(luceneIndex), ngSchema, outDupFile2, outNoDupFile1)

    // Take duplicate no duplicated documents between (pipe file and itself) and (pipe file and Dedup index)
    CheckDuplicated.takeNoDuplicated(ngSchema, outNoDupFile1 + "_self" , outNoDupFile1, outNoDupFile2)

    // Delete pre-processed output file
    CheckDuplicated.deleteFile(new File(outNoDupFile1 + "_self"))
  }
}

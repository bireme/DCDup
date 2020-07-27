/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.File

import br.bireme.ngrams.NGSchema

/** Check a DeDup input piped file against itself to look for duplicated documents.
  *
  * author: Heitor Barbieri
  * date: 20190328
  */
object SelfCheckDuplicated extends App {
  private def usage(): Unit = {
    System.err.println("usage: SelfCheckDuplicated " +
      "\n\t-pipe=<pipeFile> - DeDup piped input file" +
      "\n\t-schema=<confFile> - DeDup schema file" +
      "\n\t-outDupFile=<outDupFile> - duplicated records found in pipe file" +
      "\n\t-outNoDupFile=<outNoDupFile> - no duplicated records between pipe file and itself" +
      "\n\t[-pipeEncoding=<pipeFileEncoding>] - pipe file character encoding. Default is utf-8" +
      "\n\t[-schemaEncoding=<schemaFileEncoding>] - DeDup schema file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 4) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }
  val keys = parameters.keys.toSet
  if (!Set("pipe", "schema", "outDupFile", "outNoDupFile").forall(keys.contains)) usage()

  val pipe = parameters("pipe")
  val schema = parameters("schema")
  val outDupFile = parameters("outDupFile")
  val outNoDupFile = parameters("outNoDupFile")
  val pipeEncoding = parameters.getOrElse("pipeEncoding", "utf-8")
  val schemaEncoding = parameters.getOrElse("schemaEncoding", "utf-8")
  val ngSchema = new NGSchema(schema, schema, schemaEncoding)

  // Verifying pipe file integrity
  println("\nVerifying pipe file integrity")
  val goodFileName = File.createTempFile("good", "").getPath
  val badFileName = File.createTempFile("bad", "").getPath
  val (good,bad) = VerifyPipeFile.checkLocal(pipe, pipeEncoding, schema, goodFileName, badFileName, schemaEncoding)
  println(s"Using $good documents")
  if (bad > 0) println(s"Skipping $bad documents. See file: $badFileName\n")

  CheckDuplicated.checkDuplicated(goodFileName, "utf-8", None, ngSchema, outDupFile, outNoDupFile, selfCheck = true)
}

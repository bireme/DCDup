/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import br.bireme.ngrams.NGSchema

/** Check a DeDup input piped file against itself to look for duplicated documents.
  *
  * author: Heitor Barbieri
  * date: 20190328
  */
object SelfCheckDuplicated extends App {
  private def usage(): Unit = {
    System.err.println("usage: SelfCheckDuplicated " +
      "\n\t-pipeFile=<pipeFile> - DeDup piped input file" +
      "\n\t-confFile=<confFile> - DeDup fields configuration file" +
      "\n\t-outDupFile=<outDupFile> - duplicated records found in pipe file" +
      "\n\t-outNoDupFile=<outNoDupFile> - no duplicated records between pipe file and itself" +
      "\n\t[-pipeFileEncod=<pipeFileEncoding>] - pipe file character encoding. Default is utf-8" +
      "\n\t[-confFileEncod=<confFileEncoding>] - DeDup fields configuration file character encoding. Default is utf-8" +
      "\n\t[-outDupEncod=<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 4) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  val pipeFile = parameters("pipeFile")
  val confFile = parameters("confFile")
  val outDupFile = parameters("outDupFile")
  val outNoDupFile = parameters("outNoDupFile")
  val pipeFileEncod = parameters.getOrElse("pipeFileEncod", "utf-8")
  val confFileEncod = parameters.getOrElse("confFileEncod", "utf-8")
  val outDupEncod = parameters.getOrElse("outDupEncod", "utf-8")
  val ngSchema = new NGSchema(confFile, confFile, confFileEncod)

  CheckDuplicated.checkDuplicated(pipeFile, pipeFileEncod, None, ngSchema, outDupFile, outNoDupFile, outDupEncod)
}

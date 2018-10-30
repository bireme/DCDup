/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

/** Checks a DeDup input piped file against itself to look for duplicated
  * documents.
  *
  * author: Heitor Barbieri
  * date: 20170803
*/
object LocalCheckDuplicated extends App {
  private def usage(): Unit = {
    System.err.println("usage: LocalCheckDuplicated " +
      "\n\t<schemaFile> - DeDup schema file" +
      "\n\t<schemaFileEncoding> - schema file character encoding" +
      "\n\t<pipeFile> - DeDup piped input file" +
      "\n\t<pipeFileEncoding> - pipe file character encoding" +
      "\n\t<outDupFile> - duplicated records found in pipe file" +
      "\n\t[<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.size < 5) usage()

  val outEncoding = if (args.size > 5) args(5) else "utf-8"

  val dup = new WebDoubleCheckDuplicated()
  dup.localCheck(args(0), args(1), args(2), args(3), args(4), outEncoding)
}

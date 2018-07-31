
/*=========================================================================

    Copyright © 2016 BIREME/PAHO/WHO

    This file is part of DCDup.

    DCDup is free software: you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public License as
    published by the Free Software Foundation, either version 2.1 of
    the License, or (at your option) any later version.

    DCDup is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with DCDup. If not, see <http://www.gnu.org/licenses/>.

=========================================================================*/

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

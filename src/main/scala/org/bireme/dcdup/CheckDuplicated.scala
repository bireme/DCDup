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

import br.bireme.ngrams.{NGrams,NGIndex,NGSchema}

/** Checks a DeDup input piped file against a Dedup index (usually LILACS) to
  * look for duplicated docs.
  *
  * author: Heitor Barbieri
  * date: 20161208
*/
object CheckDuplicated extends App {
  private def usage(): Unit = {
    System.err.println("usage: CheckDuplicated " +
      "\n\t<pipeFile> - DeDup piped input file" +
      "\n\t<pipeFileEncoding> - pipe file character encoding" +
      "\n\t<luceneIndex> - path to Dedup's Lucene index" +
      "\n\t<confFile> - DeDup fields configuration file" +
      "\n\t<confFileEncoding> - DeDup fields configuration file character encoding" +
      "\n\t<outDupFile> - duplicated records found between pipe file and Dedup index" +
      "\n\t[<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 6) usage()
  val outEncoding = if (args.length > 6) args(6) else "ISO-8859-1"

  check(args(0), args(1), args(2), args(3), args(4), args(5), outEncoding)

  def check(pipeFile: String,
            pipeFileEncoding: String,
            luceneIndex: String,
            confFile: String,
            confFileEncoding: String,
            outDupFile: String,
            outDupFileEncoding: String): Unit = {
    val ngSchema = new NGSchema(confFile, confFile, confFileEncoding)
    val ngIndex = new NGIndex(luceneIndex, luceneIndex, true)

    // Check using given Lucene indexPath
    check(ngIndex, ngSchema, pipeFile, pipeFileEncoding, outDupFile,
                                                             outDupFileEncoding)
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
}

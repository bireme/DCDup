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

import br.bireme.ngrams.NGSchema

import java.io.Writer
import java.nio.file.{Files,Paths}
import java.nio.charset.Charset

import scala.collection.JavaConverters._

import org.apache.lucene.document.Document
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.{DirectoryReader,MultiFields}

object Lucene2Pipe extends App {
  private def usage(): Unit = {
    System.err.println("usage: Lucene2Pipe" +
      "\n\t<indexPath> - NGram's Lucene index path" +
      "\n\t<schemaFile> - NGram schema file" +
      "\n\t<schemaEncoding> - NGram schema file encoding" +
      "\n\t<pipeFile> - output pipe file" +
      "\n\t[<pipeEncoding>] - output pipe file encoding. Default is utf-8)"
    )
    System.exit(1)
  }

  if (args.length < 4) usage()
  val pipeEncoding = if (args.length > 4) args(4) else "utf-8"

  toPipe(args(0), args(1), args(2), args(3), pipeEncoding)

  def toPipe(indexPath: String,
             schemaFile: String,
             schemaEncoding: String,
             pipeFile: String,
             pipeEncoding: String): Unit = {

    val schema = new NGSchema("schema", schemaFile, schemaEncoding)
    val fields = mapAsScalaMap(schema.getPosNames()).toList
    val out = Files.newBufferedWriter(Paths.get(pipeFile),
                                      Charset.forName(pipeEncoding))
    val directory = FSDirectory.open(Paths.get(indexPath))
    val reader = DirectoryReader.open(directory)
    val liveDocs = MultiFields.getLiveDocs(reader)

    (0 until reader.maxDoc()).foreach {
    //(0 until 1).foreach {
      id =>
        if ((liveDocs == null) || liveDocs.get(id))
          exportDoc(reader.document(id), fields, out)
        if (id % 100000 == 0) println(s"+++$id")
    }

    reader.close()
    directory.close()
    out.close()
  }

  private def exportDoc(doc: Document,
                        fields: List[(Integer,String)],
                        out: Writer): Unit = {
    val pipe = fields.zipWithIndex.foldLeft[String]("") {
      case (str, ((pos,fldName), idx)) => {
        val str1 = (idx until pos).foldLeft[String](str) {
          case (str2, _) => str2 + "|"
        }
        val content = doc.get(fldName + "~notnormalized")
        str1 + (if (str1.isEmpty) "" else "|") +
                              (if (content == null) "" else content)
      }
    }
    out.write("\n")
    out.write(pipe)
  }
}

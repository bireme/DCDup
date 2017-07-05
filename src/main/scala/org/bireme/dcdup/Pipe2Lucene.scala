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

import scala.io.Source

object Pipe2Lucene extends App {
  private def usage(): Unit = {
    System.err.println("usage: Pipe2Lucene" +
      "\n\t<indexPath> - NGram's Lucene index path" +
      "\n\t<schemaFile> - NGram schema file" +
      "\n\t<schemaEncoding> - NGram schema file encoding" +
      "\n\t<pipeFile> - pipe file" +
      "\n\t[-encoding=<pipeEncoding>] - pipe file encoding. Default is utf-8" +
      "\n\t[--append] - append documents to an existing Lucene index"
    )
    System.exit(1)
  }

  if (args.length < 4) usage()

  val parameters = args.drop(4).foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
    }
  }

  val pipeEncoding = parameters.getOrElse("encoding", "utf-8")
  val append = parameters.contains("append")

  toLucene(args(0), args(1), args(2), args(3), pipeEncoding, append)

  def toLucene(indexPath: String,
               schemaFile: String,
               schemaEncoding: String,
               pipeFile: String,
               pipeEncoding: String,
               append: Boolean): Unit = {

    val index = new NGIndex(indexPath, indexPath, false)
    val writer = index.getIndexWriter(append)
    val schema = new NGSchema("schema", schemaFile, schemaEncoding)
    val reader = Source.fromFile(pipeFile, pipeEncoding)

    reader.getLines().zipWithIndex.foreach {
      case (line,idx) =>
        if (idx % 10000 == 0) println(s"+++$idx")
        NGrams.indexDocument(index, writer, schema, line)
    }

    writer.flush()
    print("Optimizing index ...")
    writer.forceMerge(1) // optimize index
    println("OK")
    writer.close()
    reader.close()
  }
}

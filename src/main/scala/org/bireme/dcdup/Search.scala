/*=========================================================================

    Copyright © 2017 BIREME/PAHO/WHO

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

import java.nio.file.Paths

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory

import scala.collection.JavaConverters._

/**
  * Search for documents into a Lucene index
  *
  */
object Search extends App {
  private def usage(): Unit = {
    System.err.println("usage: Search" +
      "\n\t<indexPath> - Lucene index path" +
      "\n\t<expression> - search expression" +
      "\n\t[-outFields=<fld1>,<fld2>,...,<fldn>] - Document fields to be shown. Default is all" +
      "\n\t[-count=<num>] - Number of documents to be shown. Default is all"
    )
    System.exit(1)
  }

  if (args.length < 2) usage()

  val parameters = args.drop(2).foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      if (split.length == 2)
        map + ((split(0).substring(1), split(1)))
      else {
        usage()
        map
      }
    }
  }

  val count = parameters.getOrElse("count", "9999").toInt
  val outFields = parameters.getOrElse("outFields", "")
  val fields = outFields.split(" *, *").foldLeft[Set[String]](Set()) {
    case (set, elem) => set + elem
  }

  search(args(0), args(1), fields, count)

  def search(indexPath: String,
             expression: String,
             fields: Set[String],
             count: Int): Unit = {
    val directory = FSDirectory.open(Paths.get(indexPath))
    val direader = DirectoryReader.open(directory)
    val isearcher = new IndexSearcher(direader)
    val analyzer = new StandardAnalyzer()
    val parser = new QueryParser("", analyzer)
    val query = parser.parse(expression)
    val hits = isearcher.search(query, count).scoreDocs

    println(s"Total hits:${hits.size}\n")

    hits.foreach {
      hit =>
        val doc = isearcher.doc(hit.doc)
        if (fields.isEmpty)
          doc.getFields().asScala.foreach {
            ie =>
              println("=====================================================")
              println(s"[${ie.name}]:${ie.stringValue}")
          }
        else fields.foreach {
          fld => doc.getFields(fld).foreach {
            ie =>
              println("=====================================================")
              println(s"[${ie.name}]:${ie.stringValue}")
          }
        }
    }
    direader.close()
    directory.close()
  }
}

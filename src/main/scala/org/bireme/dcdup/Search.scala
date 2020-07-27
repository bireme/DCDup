/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.nio.file.Paths

import br.bireme.ngrams.NGAnalyzer
import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, IndexableField}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc}
import org.apache.lucene.store.FSDirectory

import scala.jdk.CollectionConverters._   //scala 2.13.0
//import scala.collection.JavaConverters._

/**
  * Search for documents into a Lucene index
  *
  */
object Search extends App {
  private def usage(): Unit = {
    System.err.println("usage: Search" +
      "\n\t-index=<indexPath> - Lucene index path" +
      "\n\t-expr=<expression> - Search expression" +
      "\n\t-defField=<def>    - Default search field" +
      "\n\t[-outFields=<fld1>,<fld2>,...,<fldn>] - Document fields to be shown. Default is all" +
      "\n\t[-count=<num>]     - Number of documents to be shown. Default is all"
    )
    System.exit(1)
  }

  if (args.length < 3) usage()

  val parameters: Map[String, String] = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 2)
        map + ((split(0).substring(1), split(1)))
      else {
        usage()
        map
      }
  }
  val keys = parameters.keys.toSet
  if (!Set("index", "expr", "defField").forall(keys.contains)) usage()

  val index: String = parameters("index")
  val expr: String = parameters("expr")
  val defField: String = parameters("defField")
  val count: Int = parameters.getOrElse("count", "9999").toInt
  val outFields: String = parameters.getOrElse("outFields", "")
  val fields: Set[String] = outFields.split(" *, *").foldLeft[Set[String]](Set()) {
    case (set, elem) => if (elem.isEmpty) set else set + elem
  }

  search(index, expr, defField, fields, count)

  def search(indexPath: String,
             expression: String,
             defField: String,
             fields: Set[String],
             count: Int): Unit = {
    val directory: FSDirectory = FSDirectory.open(Paths.get(indexPath))
    val direader: DirectoryReader = DirectoryReader.open(directory)
    val isearcher: IndexSearcher = new IndexSearcher(direader)
    val analyzer: NGAnalyzer = new NGAnalyzer(true)//new StandardAnalyzer()
    val parser: QueryParser = new QueryParser(defField, analyzer)
    val query: Query = parser.parse(expression)
    val hits: Array[ScoreDoc] = isearcher.search(query, Integer.MAX_VALUE).scoreDocs

    println(s"Total hits:${hits.length}\n")

    hits.take(count).foreach {
      hit =>
        val doc: Document = isearcher.doc(hit.doc)
        println(s"\n+++ Document[${hit.doc}] +++")
        if (fields.isEmpty)
          doc.getFields().asScala.foreach {
            ie: IndexableField => println(s"[${ie.name}]:${ie.stringValue}")
          }
        else fields.foreach {
          fld => doc.getFields(fld).foreach {
            ie: IndexableField => println(s"[${ie.name}]:${ie.stringValue}")
          }
        }
    }
    direader.close()
    directory.close()
  }
}

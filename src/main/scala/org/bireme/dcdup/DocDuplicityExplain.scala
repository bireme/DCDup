/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import br.bireme.ngrams._
import org.apache.commons.text.StringEscapeUtils
import org.apache.lucene.document.Document
import org.apache.lucene.index.Term
import org.apache.lucene.search.{IndexSearcher, TermQuery}
import org.apache.lucene.search.spell.NGramDistance

import scala.collection.mutable
import scala.collection.JavaConverters._
//import scala.jdk.CollectionConverters._  // scala 2.13.0

/**
* Object used to explain if a given input piped string representing a document is duplicated or not compared to another one
  * stored in a Lucene index (created from NGrams library) and represented by a document id.
  *
  * Author: Heitor Barbieri
  * Date: 20190227
  */
object DocDuplicityExplain extends App {
  private def usage(): Unit = {
    System.err.println("usage: DocDuplicityExplain")
    System.err.println("\t\t-index=<indexPath> - Lucene index name/path")
    System.err.println("\t\t-conf=<confFile> - xml configuration/schema file")
    System.err.println("\t\t-id=<indexDocId> - id of the indexed document to compare")
    System.err.println("\t\t-doc=<pipeInputDocument> - input piped document to compare")
    System.err.println("\t\t[-encod=<confFileEncoding>] - xml configuration/schema file encoding")
    System.exit(1)
  }

  val parameters = args.foldLeft[Map[String, String]](Map()) {
    case (map, par) =>
      if (par.isEmpty) map
      else {
        val split = par.split(" *= *", 2)
        if (split.length == 1) {
          println(s"split(0)=${split(0)}")
          map + ((split(0).substring(2), ""))
        }
        else if (split.length == 2) map + ((split(0).substring(1), split(1)))
        else {
          usage()
          map
        }
      }
  }

  if (parameters.size < 4) usage()

  val index = parameters("index")
  val conf = parameters("conf")
  val id = parameters("id")
  val doc = parameters("doc")
  val encod = parameters.getOrElse("encod", "utf-8")
  val expl = explain(index, conf, encod, id, doc)

  println(expl)

  /**
  * Explain if two documents are duplicated or not and why
    * @param index lucene index path having one document
    * @param conf NGrams/DeDup schema file
    * @param encod NGrams/DeDup schema file encoding
    * @param id identifier of the indexed document used to compare for duplicity
    * @param doc input piped document used to compare for duplicity
    * @return the report saying if the documents are duplicated or not and why
    */
  def explain(index: String,
              conf: String,
              encod: String,
              id: String,
              doc: String): String = {
    val ngIndex: NGIndex = new NGIndex("index", index, true)
    val schema: NGSchema = new NGSchema("schema", conf, encod)
    val searcher: IndexSearcher = ngIndex.getIndexSearcher

    parseInputDoc(doc, schema.getParameters) match {
      case Some(doc2) => getDocument(id, searcher) match {
        case Some(document) => checkAllFields(ngIndex, schema, doc2, document)
        case None => s"Document id=$id not found."
      }
      case None => s"Parsing document error => $doc"
    }
  }

  /**
  *  Check each field of the input and indexed document, to see if they are similar of not
    * @param ngIndex object representing the lucene index in the NGrams library
    * @param schema object representing the document schema in the NGrams library
    * @param doc input piped document used to check its fields
    * @param doc2 indexed lucene document used to check its fields
    * @return the report saying if the documents are duplicated or not and why
    */
  private def checkAllFields(ngIndex: NGIndex,
                             schema: NGSchema,
                             doc: Array[String],
                             doc2: Document): String = {
    val analyzer: NGAnalyzer = ngIndex.getAnalyzer.asInstanceOf[NGAnalyzer]
    val ngDistance = new NGramDistance(analyzer.getNgramSize)
    val params: Parameters = schema.getParameters
    //val fields: Map[Int, Field] = params.getSearchFields.asScala.map[Int,Field](kv => (kv._1.toInt, kv._2)).toMap //scala 2.13.0
    val fields: Map[Int, Field] = params.getSearchFields.asScala.map(kv => (kv._1.toInt, kv._2)).toMap
    val fields2: java.util.Map[String, Field] = params.getNameFields
    val results: Seq[(Int,Int)] = fields.foldLeft(Seq[(Int,Int)]()) {
      case (seq, fld: (Int, Field)) =>
        val check: Int = NGrams.checkField(ngDistance, fld._2, doc, fields2, doc2)
        seq :+ ((fld._1, check))
    }
    val idxFldPos: Int = params.getIndexedPos
    val similarity: Float = getSimilarity(doc, idxFldPos, doc2, fields(idxFldPos).name, ngDistance)

    createReport(params, similarity, results, fields, doc, doc2)
  }

  /**
  * Get the string similarity between the input text and the indexed field
    * @param doc1 the input document
    * @param idxFldPos the position of the input text to be compared
    * @param doc2 the indexed document
    * @param idxName the name of the indexed field
    * @param ngDistance the object that will compare the distance
    * @return the distance (similarity) of the two document strings
    */
  private def getSimilarity(doc1: Array[String],
                            idxFldPos: Int,
                            doc2: Document,
                            idxName: String,
                            ngDistance: NGramDistance): Float = {
    val content1: String = br.bireme.ngrams.Tools.limitSize(
      br.bireme.ngrams.Tools.normalize(doc1(idxFldPos), NGrams.OCC_SEPARATOR),
      NGrams.MAX_NG_TEXT_SIZE).trim
    val content2: String = {
      val contents = doc2.getValues(idxName)
      if (contents.isEmpty) "" else contents.head
    }
    ngDistance.getDistance(content1, content2)
  }

  /**
  * Create an output report explaining why the two documents are duplicated or not
    * @param parameters the representation of the index schema file
    * @param similarity number indication how much two document are similar
    * @param results the result of comparing the two document fields (field position, fld check result:(-2,-1,0,1))
    * @param fields the (position, field) composition of the documents
    * @param doc1 input piped document
    * @param doc2 indexed document
    * @return report explaining if the document are duplicated or not and why
    */
  private def createReport(parameters: Parameters,
                           similarity: Float,
                           results: Seq[(Int,Int)],
                           fields: Map[Int, Field],
                           doc1: Array[String],
                           doc2: Document): String = {
    val builder: StringBuilder =  new StringBuilder()
    val sortedResult: Seq[(Int,Int)] = results.sortWith((res1, res2) => res1._1 <= res2._1)

    getDocs(doc1, doc2, fields, builder)

    val (matchedFields: Int, maxScore: Boolean) = sortedResult.foldLeft[(Int, Boolean)](0, false) {
      case ((tot, bool), (pos, result)) =>
        result match {
          case -1 =>
            builder.append(s"\nField[$pos,'${fields(pos).name}']: does NOT match")
            (tot, bool)
          case -2 =>
            builder.append(s"\nField[$pos,'${fields(pos).name}']: requires maximum similarity score")
            (tot, true)
          case 0 =>
            builder.append(s"\nField[$pos,'${fields(pos).name}']: is ignored")
            (tot, bool)
          case _ =>
            builder.append(s"\nField[$pos,'${fields(pos).name}']: matches")
            (tot + 1, bool)
        }
    }
    builder.append(s"\n\nSimilarity = $similarity")
    builder.append(s"\nRequired_max_similarity = $maxScore\nTotal_fields = ${results.size}\nMatched_fields = $matchedFields")

    getMinFields(parameters, similarity) match {
      case Some(minFields) =>
        builder.append(s"\nRequired_fields_match = $minFields")
        if (maxScore) {
          if (similarity == 1.0) {
            if (matchedFields >= minFields) builder.append("\n\n=> Documents are duplicated.")
            else builder.append("\n\n=> Documents are NOT duplicated. Matched fields[matchedFields] < $minFields.")
          } else builder.append("\n\n=> Documents are NOT duplicated. Similarity[$similarity] < 1.0")
        } else {
          if (matchedFields >= minFields) builder.append("\n\n=> Documents are duplicated.")
          else builder.append(s"\n\n=> Documents are NOT duplicated. Matched fields[matchedFields] < $minFields.")
        }
      case None => builder.append(s"\n\n=> Documents are NOT duplicated. Similarity[$similarity] < ${parameters.getMinSimilarity}.")
    }
    builder.toString()
  }

  /**
  *
    * @param doc1 input piped document
    * @param doc2 indexed similar document
    * @param fields name of the fields of the input and indexed document
    * @param builder auxiliary string to append the both document fields
    * @return a String with the input text fields and the similar document fields
    */
  private def getDocs(doc1: Array[String],
                      doc2: Document,
                      fields: Map[Int, Field],
                      builder: StringBuilder): StringBuilder = {
    builder.append("\nInput document:")
    fields foreach {
      case (pos, fld) => builder.append(s"\n${fld.name} = ${doc1(pos)}")
    }

    builder.append("\n\nIndexed document:")
    fields foreach {
      case (_, fld) =>
        val notNormalized: String = fld.name + "~notnormalized"
        builder.append(s"\n${fld.name} = ${doc2.getValues(notNormalized).head}")
    }
    builder.append("\n")
  }

  /**
  * Giving a schema file, looks for the minimum number of matching fields for a given similarity
    * @param parameters object representing the schema file
    * @param similarity the distance from two documents
    * @return the minimum number of matching fields of two documents required to be considered duplicated
    */
  private def getMinFields(parameters: Parameters,
                           similarity: Float): Option[Int] = {
    val scores: mutable.Seq[Score] = {
      val buffer = mutable.Buffer.empty[Score]
      val iterator = parameters.getScores.iterator()

      while (iterator.hasNext) buffer += iterator.next()
      buffer
    }
    scores.find(score => score.getMinValue <= similarity).map(_.getMinFields)
  }

  /**
  * Parse a piped document string and convert it into a array
    * @param doc input document
    * @param params schema
    * @return an array with document fields where the position is the same of the piped string
    */
  private def parseInputDoc(doc: String,
                            params: Parameters): Option[Array[String]] = {
    require(doc != null)

    val split: Array[String] = StringEscapeUtils.unescapeHtml4(doc).replace(':', ' ').trim
                                                   .split(" *\\| *", Integer.MAX_VALUE)
    //val fields: Map[Int, Field] = params.getSearchFields.asScala.map[Int,Field](kv =>  (kv._1.toInt, kv._2)).toMap //scala 2.13.0
    val fields: Map[Int, Field] = params.getSearchFields.asScala.map(kv =>  (kv._1.toInt, kv._2)).toMap

    if (split.length == fields.size) Some(split)
    else None
  }

  /**
  * Retrieve a document whose field 'id' is docId
    * @param docId document string identification
    * @param searcher IndexSearcher Lucene object
    * @return the Lucene document whose 'id' field is docId
    */
  private def getDocument(docId: String,
                          searcher: IndexSearcher): Option[Document] = {
    val idn: String = br.bireme.ngrams.Tools.limitSize(
      br.bireme.ngrams.Tools.normalize(docId, NGrams.OCC_SEPARATOR), NGrams.MAX_NG_TEXT_SIZE).trim
    val query = new TermQuery(new Term("id", idn))

    val topDocs = searcher.search(query, 1)
    if (topDocs.totalHits.value == 0) None
    else Some(searcher.doc(topDocs.scoreDocs.head.doc))
  }
}

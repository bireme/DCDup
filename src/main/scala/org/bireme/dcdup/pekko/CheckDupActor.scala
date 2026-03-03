/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup.pekko

import java.util
import org.apache.pekko.actor.{Actor, ActorRef}
import br.bireme.ngrams._
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.spell.NGramDistance

import scala.collection.{immutable, mutable}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class CheckDupActor(ngIndex: NGIndex,
                    ngSchema: NGSchema,
                    id_id: util.Set[String],
                    reader: ActorRef,
                    writer: ActorRef,
                    teller: ActorRef,
                    selfCheck: Boolean) extends Actor {
  // Create objects required to check documents
  val analyzer: NGAnalyzer = ngIndex.getAnalyzer.asInstanceOf[NGAnalyzer]
  val parameters: Parameters = ngSchema.getParameters
  val results: util.List[NGrams.Result] = new java.util.ArrayList[NGrams.Result]()
  val searcher: IndexSearcher = ngIndex.getIndexSearcher
  val size: Int = ngSchema.getNamesPos.size
  val ngDistance: NGramDistance = new NGramDistance(analyzer.getNgramSize)

  override def receive: Receive = {
    case Start => reader ! AskByDoc

    case doc: String =>
      val ttext: String = doc.replace(':', ' ').trim
      //println(s"ttext=$ttext")
      results.clear()

      Try {
        NGrams.searchRaw(parameters, searcher, analyzer, ngDistance, ttext, true, selfCheck, id_id, results)
        if (results.isEmpty) {
          //println("Result is empty!")
          Set[String]()
        } else {
          //println(s"size=${id_id.size} $self.path.name] line[$doc]")
          //NGrams.result2PipeReport(parameters, results).asScala.toSet
          result2PipeReport(parameters, results, ttext)
        }
      } match {
        case Success(resSet) =>
          if (resSet.nonEmpty) writer ! resSet
        case Failure(_) =>
          println(s"Line with error:[$ttext]")
          //ex.printStackTrace()//System.err.println(ex)
      }
      teller ! PRINT
      reader ! AskByDoc

    case Finish =>
      //println("CheckUrlActor finishing.")
      searcher.getIndexReader.close()
  }

  private def result2PipeReport(parameters: Parameters,
                                results: util.List[NGrams.Result],
                                inLine: String): Set[String] = {
    val ret: mutable.TreeSet[String] = mutable.TreeSet[String]()

    results.asScala.foreach {
      result =>
        val builder: StringBuilder = new StringBuilder()
        var first: Boolean = true

        result.resltList.asScala.foreach {
          fcr =>
            if (first) first = false
            else builder.append("|")

            builder.append(fcr.elem1).append("|").append(fcr.elem2)
              .append("|").append(fcr.similarity)
              .append("|").append(fcr.condition)
        }

        ret.add(builder.toString())
    }
    immutable.TreeSet.from(ret)(using Ordering.String.reverse)
  }
}

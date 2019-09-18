/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup.akka

import java.util

import akka.actor.{Actor, ActorRef}
import br.bireme.ngrams._
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.spell.NGramDistance

import scala.collection.JavaConverters._
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
  val results: util.HashSet[NGrams.Result] = new java.util.HashSet[NGrams.Result]()
  val searcher: IndexSearcher = ngIndex.getIndexSearcher
  val size: Int = ngSchema.getNamesPos.size
  val ngDistance: NGramDistance = new NGramDistance(analyzer.getNgramSize)

  override def receive: Receive = {
    case Start => reader ! AskByDoc

    case doc: String =>
      val ttext: String = doc.replace(':', ' ').trim
      results.clear()

      Try {
        NGrams.searchRaw(parameters, searcher, analyzer, ngDistance, ttext, true, selfCheck, id_id, results)
        if (results.isEmpty) Set[String]()
        else {
          //println(s"size=${id_id.size} $self.path.name] line[$doc]")
          NGrams.results2pipe(parameters, results).asScala.toSet
        }
      } match {
        case Success(resSet) =>
          if (resSet.nonEmpty) writer ! resSet
        case Failure(ex) =>
          println(s"Line with error:[$ttext]")
          ex.printStackTrace()//System.err.println(ex)
      }
      teller ! PRINT
      reader ! AskByDoc

    case Finish =>
      //println("CheckUrlActor finishing.")
      searcher.getIndexReader.close()
  }
}
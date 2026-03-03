/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup.pekko

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import br.bireme.ngrams.{Field, NGIndex, NGSchema}
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.TreeMap
import scala.collection.{immutable, mutable}
import scala.concurrent.duration.{Duration, HOURS}
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.MapHasAsScala

case object Start
case object Finish
case object AskByDoc

object CheckDuplicatedPekko {
  /**
  *
    * @param ngIndex NGrams index path
    * @param ngSchema NGrams index/search configuration file
    * @param pipeFile input piped file used to look for similar docs
    * @param pipeFileEncoding input piped file character encoding
    * @param outDupFile output piped file with the similar documents
    * @param numberOfCheckers number of threads doing the check
    * @param selfCheck if true indicate that it is a duplicated check from input file against itself
    */
  def check(ngIndex: NGIndex,
            ngSchema: NGSchema,
            pipeFile: String,
            pipeFileEncoding: String,
            outDupFile: String,
            numberOfCheckers: Int,
            selfCheck: Boolean): Unit = {
    assert (numberOfCheckers > 0)

    // Create the environment to run and manage checkers
    val _system = ActorSystem.create("CheckDuplicatedPekko",
      ConfigFactory.load("application"))

    // Create actor that will write duplicated documents into output file
    val mutableMap: mutable.Map[Integer, Field] = ngSchema.getParameters.getSearchFields.asScala
    val treeMap: TreeMap[Integer, Field] = immutable.TreeMap.from(mutableMap)
    val writeDup: ActorRef = _system.actorOf(Props(classOf[WriteDupActor], new File(outDupFile), treeMap), "writeDup")

    // Create actor that will read documents to be checked from input file
    val readDup: ActorRef = _system.actorOf(Props(classOf[ReadDupActor],
      new File(pipeFile), pipeFileEncoding, writeDup, numberOfCheckers), name = "readDup")

    // Create actor that will print the current document into the scream
    val teller: ActorRef = _system.actorOf(Props(classOf[Teller]), name = "teller")

    // Create duplicated documents checkers
    val map = new java.util.concurrent.ConcurrentHashMap[String, Byte]()
    val id_id: ConcurrentHashMap.KeySetView[String, Byte] = map.keySet(0)
    val checkers = collection.mutable.Buffer[ActorRef]()
    1 to numberOfCheckers foreach {
      idx => checkers +=
        _system.actorOf(Props(classOf[CheckDupActor], ngIndex, ngSchema, id_id, readDup, writeDup, teller, selfCheck),
          "checker" + idx)
    }

    // Start checking
    checkers.foreach(_ ! Start)

    // Wait the check finish or timeout
    implicit val timeout: Timeout = Timeout(Duration(50, HOURS))
    val future: Future[Any] = readDup ? Start
    Await.result(future, Duration(50, HOURS))

    //println("CheckDuplicatedPekko is finishing")

    // Stop threads and clean resources
    _system.terminate()
  }
}



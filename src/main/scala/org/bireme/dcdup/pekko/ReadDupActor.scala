/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup.pekko

import java.io.File

import org.apache.pekko.actor.{Actor, ActorRef}

import scala.io.{BufferedSource, Source}

class ReadDupActor(file: File,
                   codec: String,
                   writeDup: ActorRef,
                   numOfCheckers: Int) extends Actor {
  private val src: BufferedSource = Source.fromFile(file)(codec)
  private val lines: Iterator[String] = src.getLines()
  private var ref : ActorRef = _
  private var finished: Int = 0

  override def receive: Receive = {
    case Start =>
      ref = sender()

    case AskByDoc =>
      if (lines.hasNext) {
        sender() ! lines.next()
      } else {
        sender() ! Finish
        finished += 1
        if (finished == numOfCheckers) writeDup ! Finish
      }

    case Finish =>
      //println("ReadDupActor is finishing")
      src.close()
      ref ! Finish   // Advise CheckDuplicatedPekko to finish
  }

  /*def killCheckers(): Unit = {
  }*/
}

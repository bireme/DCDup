/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup.pekko

import org.apache.pekko.actor.Actor

case object PRINT

class Teller extends Actor {
  var cur: Int = 0

  override def receive: Receive = {
    case PRINT =>
      if (cur % 1000 == 0) println(s"<<< $cur")
      cur += 1
  }
}

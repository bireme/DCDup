package org.bireme.dcdup

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}

object Apagar2 extends App {
  private def usage(): Unit = {
    System.err.println("Apagar <filename> (<UTF8>|<ISO8859-1>)")
    System.exit(1)
  }
  if (args.size != 2) usage()

  val filename = args(0)
  val encoding = args(1)
  if (!encoding.equals("UTF8") && !encoding.equals("ISO8859-1")) usage()

  val codec = args(1) match {
    case "ISO8859-1" => Codec.ISO8859
    case _           => Codec.UTF8
  }
  val codAction = CodingErrorAction.REPORT
  val decoder = codec.decoder.onMalformedInput(codAction)

  val src = Source.fromFile(filename)(decoder)
  val lines = src.getLines()
  var continue = true

  while(continue) {
    try {
      if (lines.hasNext) println(lines.next())
      else continue = false
    } catch {
      case t: Throwable => {println(t)}
    }
  }

  src.close()
}

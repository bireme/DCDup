package org.bireme.dcdup

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}

object Apagar extends App {
  private def usage(): Unit = {
    System.err.println("Apagar <filename> (<UTF8>|<ISO8859-1>) (<IGNORE>|<REPLACE>|<REPORT>)")
    System.exit(1)
  }
  if (args.size != 3) usage()

  val filename = args(0)
  val encoding = args(1)
  if (!encoding.equals("UTF8") && !encoding.equals("ISO8859-1")) usage()

  val action = args(2)
  if (!action.equals("IGNORE") && !action.equals("REPLACE") &&
      !action.equals("REPORT")) usage()

  val codec = args(1) match {
    case "ISO8859-1" => Codec.ISO8859
    case _           => Codec.UTF8
  }
  val codAction = args(2) match {
    case "REPLACE" => CodingErrorAction.REPLACE
    case "REPORT"  => CodingErrorAction.REPORT
    case _         => CodingErrorAction.IGNORE
  }
  val decoder = codec.decoder.onMalformedInput(codAction)
  val src = Source.fromFile(filename)(decoder)
  val lines = src.getLines()

  lines.foreach(line => println(line))

  src.close()
}

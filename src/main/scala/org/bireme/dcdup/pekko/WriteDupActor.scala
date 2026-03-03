/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup.pekko

import br.bireme.ngrams.Field

import java.io.{BufferedWriter, File}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import org.apache.pekko.actor.Actor

class WriteDupActor(goodUrlFile: File,
                    sfields: Map[Integer, Field]) extends Actor {
  private val dupWriter: BufferedWriter = Files.newBufferedWriter(goodUrlFile.toPath, StandardCharsets.UTF_8,
    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

  private var first = true
  val x = sfields.values.toSeq
  sfields.values.map(_.name).foreach {
    name =>
      if (first) first = false else dupWriter.append("|")
      dupWriter.append(s"${name}_1|${name}_2|score|status")
  }
  dupWriter.append("\n")
  //dupWriter.append("rank|similarity|search_doc_id|index_doc_id|ngram_search_text|ngram_index_text|search_source|" +
  //  "index_source\n\n")

  override def receive: Receive = {
    case lines: Set[String] =>
      lines.foreach {
        line: String =>
          if (line.nonEmpty) {
            dupWriter.write(line)
            dupWriter.newLine()
          }
      }
    case Finish =>
      dupWriter.close()
      context.actorSelection("../readDup") ! Finish
      //println("WriteUrlActor is finishing")
    // finish itself
  }
}

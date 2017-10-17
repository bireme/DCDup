/*=========================================================================

    Copyright © 2017 BIREME/PAHO/WHO

    This file is part of DCDup.

    DCDup is free software: you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public License as
    published by the Free Software Foundation, either version 2.1 of
    the License, or (at your option) any later version.

    DCDup is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with DCDup. If not, see <http://www.gnu.org/licenses/>.

=========================================================================*/

package org.bireme.dcdup

import io.circe.parser._

import java.io.{BufferedWriter, IOException}
import java.nio.charset.Charset
import java.nio.file.{Files,Paths}

import scala.io._

/** Check an input piped file against a local Ngrams schema file or against
  * a remote schema file in a DeDup server.
  *
  * @author: Heitor Barbieri
  * date: 20170416
  */
object CheckSchema extends App {
  private def usage(): Unit = {
    System.err.println("usage: CheckSchema" +
      "\n\t-pipe=<pipeFile> - input piped file" +
      "\n\t[-pipeEncoding=<encoding>] - piped file encoding. Default is utf-8" +
      "\n\t( -schemaFile=<file path> - local NGrams schema file" +
      "\n\t| -schemaUrl=<DeDup url> - url of a DeDup schema )" +
      "\n\t-good=<file path> - file that contains piped lines following the schema" +
      "\n\t-bad=<file path> - file that contains piped lines that does not follow the schema"
    )
    System.exit(1)
  }

  if (args.length < 4) usage()

 // Parse parameters
  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      if (split.length == 2) map + ((split(0).substring(1), split(1)))
      else {
        usage()
        map
      }
    }
  }

  val pipe = parameters("pipe")
  val encoding = parameters.getOrElse("pipeEncoding", "utf-8")
  val schemaFile = parameters.get("schemaFile")
  val schemaUrl = parameters.get("schemaUrl")
  val good = parameters("good")
  val bad = parameters("bad")

  check(pipe, encoding, schemaFile, schemaUrl, good, bad)

  def check(pipe: String,
            encoding: String,
            schemaFile: Option[String],
            schemaUrl: Option[String],
            good: String,
            bad: String): Unit = {

    val reader = Source.fromFile(pipe, encoding)
    val lines = reader.getLines()
    val source = schemaFile match {
      case Some(file) => Source.fromFile(file, "utf-8")
      case None => schemaUrl match {
        case Some(url) => Source.fromURL(url, "utf-8")
        case None => throw new IOException("schema required")
      }
    }
    val schema = source.getLines().mkString(" ")
    val goodWriter = Files.newBufferedWriter(Paths.get(good),
                                             Charset.forName(encoding))
    val badWriter = Files.newBufferedWriter(Paths.get(bad),
                                            Charset.forName(encoding))

    checkRaw(lines, schema, goodWriter, badWriter)

    reader.close()
    source.close()
    goodWriter.close()
    badWriter.close()
  }

  private def checkRaw(lines: Iterator[String],
                       schema: String,
                       goodWriter: BufferedWriter,
                       badWriter: BufferedWriter): Unit = {
    val map = parseSchema(schema) // (pos => (presence, reqFieldPos))
    val lastIndex = map.last._1
    require(lastIndex >= 0)

    lines.foreach {
      line => if (checkLine(map, lastIndex, line)) goodWriter.write(line + "\n")
              else badWriter.write(line + "\n")
    }
  }

  private def parseSchema(schema: String): Map[Int, (Boolean, Int)] = {
    parse(schema) match {
      case Right(doc) =>
        val map1 = doc.hcursor.downField("params").values.get.
          foldLeft[Map[String, (Int,String,String)]] (Map()) {
            case (map,jelem) =>
              val cursor = jelem.hcursor
              map + (
                cursor.downField("name").as[String].getOrElse("") -> (
                  cursor.downField("pos").as[Int].getOrElse(-1),
                  cursor.downField("presence").as[String].getOrElse(""),
                  cursor.downField("requiredField").as[String].getOrElse("")
                )
              )
          }
        map1.values.foldLeft[Map[Int,(Boolean, Int)]](Map()) {
          case (map,(pos,presence,reqField)) =>
            map + (pos -> (presence.equals("required"), map1(reqField)._1))
        }
      case Left(_) => throw new IOException(s"invalid schema [$schema]")
    }
  }

  private def checkLine(map: Map[Int, (Boolean, Int)],
                        lastIndex: Int,
                        line: String): Boolean = {
    val split = line.trim.split(" *\\| *", 100)

    if (split.size != lastIndex + 1) false
    //else split.zipWithIndex.forall

    else ! split.zipWithIndex.exists {
      case (elem, index) =>
        map.get(index) match {
          case Some(schElem) =>
            (elem.isEmpty && schElem._1) || (! (map(schElem._2)._1))
          case None => true
        }
    }
  }
}

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
import java.nio.charset.{Charset, CodingErrorAction, MalformedInputException}
import java.nio.file.{Files,Paths}

import scala.collection.immutable.TreeMap
import scala.io._
import scala.util.{Try, Success, Failure}

/** Check an input piped file against a local Ngrams schema file or against
  * a remote schema file in a DeDup server.
  *
  * @author: Heitor Barbieri
  * date: 20170416
  */
object CheckPipeFile extends App {
  private def usage(): Unit = {
    System.err.println("Check an input piped file against a local Ngrams schema" +
      "\nfile or against a remote schema file in a DeDup server." +
      "\n\nusage: CheckPipeFile" +
      "\n\t-pipe=<pipeFile> - input piped file" +
      "\n\t[-pipeEncoding=<encoding>] - piped file encoding. Default is utf-8" +
      "\n\t-schemaUrl=<DeDup url> - url of a DeDup schema" +
      "\n\t-good=<file path> - file that contains piped lines following the schema" +
      "\n\t-bad=<file path> - file that contains piped lines that does not follow the schema"
    )
    System.exit(1)
  }

  if (args.length < 4) usage()
 // Parse parameters
  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 2) map + ((split(0).substring(1), split(1)))
      else {
        usage()
        map
      }
  }

  val pipe = parameters("pipe")
  val encoding = parameters.getOrElse("pipeEncoding", "utf-8")
  val schemaUrl = parameters("schemaUrl")
  val good = parameters("good")
  val bad = parameters("bad")

  Try(VerifyPipeFile.check(pipe, encoding, schemaUrl, good, bad)) match {
    case Success((goodDocs, badDocs)) =>
      println(s"Properly formatted lines: $goodDocs")
      println(s"Incorrectly formatted lines : $badDocs")
    case Failure(e) => e match {
      case _: MalformedInputException =>
        println(s"==> The encoding[$encoding] specified as '-pipeEncoding' " +
          "parameter seems not to be the same as the pipe file.")
      case _ => throw(e)
    }
  }
}

/** Check an input piped file against a local Ngrams schema file or against
  * a remote schema file in a DeDup server.
  */
object VerifyPipeFile {

  /**
    * Check an input piped file against a DeDup schema file
    *
    * @param pipe - input piped file to be checked
    * @param encoding - piped file character encoding
    * @param schemaUrl - url of the DeDup schema page. For example: http://dedup.bireme.org/services/schema/LILACS_Sas_Seven
    * @param good - path of the temporary file having lines that follow the schema
    * @param bad - path of the temporary file having lines that do not follow the schema
    * @return (number of good lines, number of bad lines)
    */
  def check(pipe: String,
            encoding: String,
            schemaUrl: String,
            good: String,
            bad: String): (Int, Int) = {
    val codec = encoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val reader = Source.fromFile(pipe)(decoder)
    val lines = reader.getLines()
    val source = Source.fromURL(schemaUrl, "utf-8")
    val schema = source.getLines().mkString(" ")
    val goodWriter = Files.newBufferedWriter(Paths.get(good),
                                             Charset.forName(encoding))
    val badWriter = Files.newBufferedWriter(Paths.get(bad),
                                            Charset.forName(encoding))
    val (goodDocs, badDocs) = checkRaw(lines, schema, goodWriter, badWriter)

    reader.close()
    source.close()
    goodWriter.close()
    badWriter.close()

    (goodDocs,badDocs)
  }

  /**
    * Check an input piped file against a DeDup schema file
    *
    * @param lines - input piped file line iterator
    * @param schema - DeDup schema content string
    * @param goodWriter - writer to the temporary file having lines that follow the schema
    * @param badWriter - writer to the temporary file having lines that do not follow the schema
    * @return (number of good lines, number of bad lines)
    */
  private def checkRaw(lines: Iterator[String],
                       schema: String,
                       goodWriter: BufferedWriter,
                       badWriter: BufferedWriter): (Int,Int) = {
    val map = parseSchema(schema) // (pos => (presence, reqFieldPos))
    val lastIndex = map.last._1
    require(lastIndex >= 0)

    lines.foldLeft[(Int,Int)] (0,0) {
      case ((good,bad),line) =>
        if (checkLine(map, lastIndex, line)) {
          goodWriter.write(line + "\n")
          (good + 1, bad)
        } else {
          badWriter.write(line + "\n")
          (good, bad + 1)
        }
    }
  }

  /**
    * Parse a DeDup schema string and convert it into a map
    *
    * @param schema DeDup schema content string
    * @return a map where the key is the field 'pos' and the value is a duple of
    *         (field is required (boolean), position of the requiredField)
    */
  private def parseSchema(schema: String): Map[Int, (Boolean, Int)] = { //pos -> (required,reqFieldPos)
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
        map1.values.foldLeft[Map[Int,(Boolean, Int)]](TreeMap()) {
          case (map,(pos,presence,reqField)) =>
            map + (pos -> (presence.toLowerCase.equals("required"),
                   if (reqField.isEmpty) -1 else map1(reqField)._1))
        }
      case Left(_) => throw new IOException(s"invalid schema [$schema]")
    }
  }

  /**
    * Check if a piped line follows the schema
    *
    * @param map as returned by parseSchema Foundation
    * @param lastIndex the biggest 'pos' field of the schema
    * @param line the line to be checked
    * @return true if the line follows the schema and false otherwise
    */
  private def checkLine(map: Map[Int, (Boolean, Int)],
                        lastIndex: Int,
                        line: String): Boolean = {
    val split = line.trim.split(" *\\| *", 100)

    if (split.size != lastIndex + 1) false
    else ! split.zipWithIndex.exists {
      case (elem, index) =>
        map.get(index).forall(
          schElem => (elem.isEmpty && schElem._1) ||
                     ((schElem._2 != -1) && split(schElem._2).isEmpty)
        )
    }
    /*else ! split.zipWithIndex.exists {
      case (elem, index) =>
        map.get(index) match {
          case Some(schElem) =>
            (elem.isEmpty && schElem._1) || (! (map(schElem._2)._1))
          case None => true
        }
    }*/
  }
}

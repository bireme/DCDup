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

import io.circe._
import io.circe.parser._

import java.io.BufferedWriter
import java.nio.charset.Charset
import java.nio.file.{Files,Paths}
import java.sql.{DriverManager,ResultSet}

import scala.io._
import scala.util.Try

/** Exports some MySQL records fields to a piped file.
*
* @author: Heitor Barbieri
* date: 20170410
*/
object MySQL2Pipe extends App {
  private def usage(): Unit = {
    System.err.println("usage: MySQL2Pipe" +
      "\n\t-host=<MySQL_Host> - MySQL host url" +
      "\n\t-user=<MySQL_User> - MySQL user" +
      "\n\t-pswd=<MySQL_Password> - MySQL password" +
      "\n\t-dbnm=<MySQL_Dbname> - MySQL database name" +
      "\n\t-sql=<sqlFile> - sql specification file" +
      "\n\t-pipe=<pipeFile> - output piped file" +
      "\n\t[-sqlEncoding=<sqlEncoding>] - sql file encoding. Default is utf-8" +
      "\n\t[-pipeEncoding=<pipeEncoding>] - output piped file encoding. Default is utf-8" +
      "\n\t[-jsonField=<tag>[,<tag>,...,<tag>]] - json field to use if the row element is a json object." +
      " It will use the first with content" +
      "\n\t[-repetitiveField=<name>[.<name>,...,<name>]] - the name of the fields that should be broken " +
      "into a new line when the repetitive separator symbol is found" +
      "\n\t[-repetitiveSep=<separator>] - repetitive field string separator. Default is //@//"
    )
    System.exit(1)
  }

  if (args.length < 6) usage()

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

  val host = parameters("host")
  val user = parameters("user")
  val pswd = parameters("pswd")
  val dbnm = parameters("dbnm")
  val sqlf = parameters("sql")
  val pipe = parameters("pipe")
  val sqlEncoding = parameters.getOrElse("sqlEncoding", "utf-8")
  val pipeEncoding = parameters.getOrElse("pipeEncoding", "utf-8")
  val jsonField = parameters.getOrElse("jsonField", "").trim.split(" *\\, *").toSet
  val repetitiveField = parameters.getOrElse("repetitiveField", "").trim.
                                                          split(" *\\, *").toSet
  val repetitiveSep = parameters.getOrElse("repetitiveSep", "//@//")

  Class.forName("com.mysql.jdbc.Driver")

  sql2pipe(host, user, pswd, dbnm, sqlf, pipe,
           sqlEncoding, pipeEncoding, jsonField, repetitiveField, repetitiveSep)

  def sql2pipe(host: String,
               user: String,
               pswd: String,
               dbnm: String,
               sqlf: String,
               pipe: String,
               sqlEncoding: String,
               pipeEncoding: String,
               jsonField: Set[String],
               repetitiveFields: Set[String],
               repetitiveSep: String): Unit = {
    val reader = Source.fromFile(sqlf, sqlEncoding)
    val content = reader.getLines().mkString(" ")
    reader.close()

    val writer = Files.newBufferedWriter(Paths.get(pipe),
                                         Charset.forName(pipeEncoding))
    val con = DriverManager.getConnection(
                                 s"jdbc:mysql://${host.trim}:3306/${dbnm.trim}",
                                                                     user, pswd)
    val rs = con.createStatement().executeQuery(content)
    val meta = rs.getMetaData()
    val cols = meta.getColumnCount()
    val names = (1 to cols).foldLeft[List[String]](List()) {
      case (lst, col) => lst :+ meta.getColumnName(col)
    }
//println(s"cols=$cols names=$names")
    while (rs.next()) {
      parseRecord(rs, names, cols, jsonField,repetitiveFields, repetitiveSep).
                                                                       foreach {
        line =>
          writer.write(line)
          writer.newLine()
      }
    }
    writer.close()
    con.close()
  }

  /** Given a record retrieved by a query, returns a list of the contents of the
    * desired fields.
    *
    * @param rs result set object whose current position points to the retrieved record
    * @param names list of the column names
    * @param cols number of record columns
    * @param jsonField if a column element is a json element, indicates which
    *  json element to retrieve the content. Use the first element of the set that
    *  matches the json document element tag
    * @param repetitiveFields name of the fields whose content will be content be
    * splited into lines at the symbol 'repetitiveSep'
    * @param repetitiveSep the string used to split a field content into more than
    * one output line (repetitive field separator)
    *
    * @return a list of otuput piped strings (only one if repetitiveFields is empty)
    */
  private def parseRecord(rs: ResultSet,
                          names: List[String],
                          cols: Int,
                          jsonField: Set[String],
                          repetitiveFields: Set[String],
                          repetitiveSep: String): List[String] = {
    (1 to cols).foldLeft[List[String]](List()) {
      case(lst,col) => {
        val str = (Try(rs.getString(col)) getOrElse "????")
        val str2 = if (str == null) "" else str.trim()
        val str3 =  if (str2.startsWith("[{") || str2.startsWith("{"))
                      getElement(str2, jsonField, repetitiveSep)
                    else str2
        val str4 = str3.replace("|", " ")
        val elems = parseCell(str4, repetitiveFields(names(col - 1)), repetitiveSep)
        if (lst.isEmpty) elems else elems.flatMap(elem => lst.map(l => s"$l|$elem"))
      }
    }
  }

  /** Given a celula content, it returns a list of one element (the content itself)
    * if splitLine is false or repetitiveSep symbol is not found or a list of
    * piped lines otherwise. For ex "[a][b][c//@//d][e]" will result:
    *  a|b|c|e and a|b|d|e for //@// repetitiveSeparator
    *
    * @param colContent the cell content (document field)
    * @param splitLine indicates if the content should be splited into line if
    *                  the repetitive separator string is found
    * @param repetitiveSep string indicating the position where the content
    *                      should be splitted
    * @return a list with one String (celContent) if celContent is not splitted
    *         or some String (celContent splitted) otherwise
    */
  private def parseCell(celContent: String,
                        splitLine: Boolean,
                        repetitiveSep: String): List[String] = {

    if (splitLine) {
      celContent.split(repetitiveSep).foldLeft[List[String]](List()) {
        case (lst, str) => lst :+ str.trim()
      }
    } else List(celContent)
  }

  /** Retrieves a json document field
    *
    * @param json json document content
    * @param jsonField field names whose content will be retrieved
    * @param repetitiveSep symbol to be used to concatenate field content which
    *                      is a listFiles
    * return a String with the json field
    */
  private def getElement(json: String,
                         jsonField: Set[String],
                         repetitiveSep: String): String = {
    if (jsonField.isEmpty) json
    else {
      val doc: Json = parse(json).getOrElse(Json.Null)
      if (doc == Json.Null) json
      else findElement(doc, jsonField, repetitiveSep) match {
        case Some(str) => str
        case None => json
      }
    }
  }

  /** Retrieves a json document field
    *
    * @param json Json document object'
    * @param jsonField field names whose content will be retrieved
    * @param repetitiveSep symbol to be used to concatenate field content which
    *                      is a listFiles
    * return Some with the field content or None otherwise
    */
  private def findElement(doc: Json,
                          jsonField: Set[String],
                          repetitiveSep: String): Option[String] = {
    if (jsonField.isEmpty) None
    else {
      val elems: List[Json] = doc.findAllByKey(jsonField.head)
      if (elems.isEmpty) findElement(doc, jsonField.tail, repetitiveSep)
      else Some(elems.map(elem => elem.asString).flatten.mkString(repetitiveSep))
    }
  }
}

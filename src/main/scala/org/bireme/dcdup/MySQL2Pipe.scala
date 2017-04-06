/*=========================================================================

    Copyright © 2016 BIREME/PAHO/WHO

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
import java.sql.DriverManager

import scala.io._
import scala.util.Try

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
      "\n\t[-repetiveSep=<separator>] - repetitive field string separator. Default is //@//"
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
  val repetitiveSep = parameters.getOrElse("repetiveSep", "//@//")

  Class.forName("com.mysql.jdbc.Driver")

  sql2pipe(host, user, pswd, dbnm, sqlf, pipe,
           sqlEncoding, pipeEncoding, jsonField, repetitiveSep)

  def sql2pipe(host: String,
               user: String,
               pswd: String,
               dbnm: String,
               sqlf: String,
               pipe: String,
               sqlEncoding: String,
               pipeEncoding: String,
               jsonField: Set[String],
               repetitiveSep: String): Unit = {
    val reader = Source.fromFile(sqlf, sqlEncoding)
    val content = reader.getLines().mkString(" ")
    reader.close()

    val writer = Files.newBufferedWriter(Paths.get(pipe),
                                         Charset.forName(pipeEncoding))
    val con = DriverManager.getConnection(
                                 s"jdbc:mysql://${host.trim}:3306/${dbnm.trim}",
                                                                     user, pswd)
//println("create")
    val stmt = con.createStatement()
//println(s"execute: $content")
    val rs = stmt.executeQuery(content)
//println("metadata")
    val cols = rs.getMetaData().getColumnCount()
//println(s"cols=$cols")
    while (rs.next()) {
      (1 to cols) foreach {
        col => {
          val str = (Try(rs.getString(col)) getOrElse "????")
          val str2 = if (str == null) "" else {
            val str3 = str.trim()
            if (str3.startsWith("[{") || str3.startsWith("{"))
              getElement(str3, jsonField, repetitiveSep)
            else str3
          }
          // build the line
          if (col > 1) writer.write("|")
          //println(str2)
          writer.write(str2.replace("|", " "))  // Eliminates pipe character
        }
      }
      //println("=====================================================")
      writer.newLine()
    }
    writer.close()
    con.close()
  }

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

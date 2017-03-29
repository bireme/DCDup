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

import java.io.BufferedWriter
import java.nio.charset.Charset
import java.nio.file.{Files,Paths}

import scala.io._
import scalikejdbc._

object MySQL2Pipe extends App {
  private def usage(): Unit = {
    System.err.println("usage: MySQL2Pipe" +
      "\n\t-host=<MySQL_Host> - MySQL host url" +
      "\n\t-user=<MySQL_User> - MySQL user" +
      "\n\t-pswd=<MySQL_Password> - MySQL password" +
      "\n\t-dbnm=<MySQL_Dbname> - MySQL database name" +
      "\n\t-sql=<sqlFile> - sql specification file" +
      "\n\t-pipe=<pipeFile> - output piped file" +
      "\n\t[-sqlEncoding=<sqlEncoding>] - sql file encoding. Default is utf-8)" +
      "\n\t[-pipeEncoding=<pipeEncoding>] - output piped file encoding. Default is utf-8)"
    )
    System.exit(1)
  }

  if (args.length < 6) usage()

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

  Class.forName("com.mysql.jdbc.Driver")

  sql2pipe(host, user, pswd, dbnm, sqlf, pipe, sqlEncoding, pipeEncoding)

  def sql2pipe(host: String,
               user: String,
               pswd: String,
               dbnm: String,
               sqlf: String,
               pipe: String,
               sqlEncoding: String,
               pipeEncoding: String): Unit = {
    val reader = Source.fromFile(sqlf, sqlEncoding)
    val content = reader.getLines().mkString(" ")
    reader.close()

    val writer = Files.newBufferedWriter(Paths.get(pipe),
                                         Charset.forName(pipeEncoding))
    ConnectionPool.singleton(s"jdbc:mysql://${host.trim}:3306/${dbnm.trim}",
                                                                     user, pswd)
    DB readOnly {
      implicit session =>
        SQL(content).foreach {
          rs =>
            if (rs.index > 1) writer.newLine()
            if (rs.index % 100 == 0) println(s"+++ ${rs.index}")

            val colCount = rs.metaData.getColumnCount()
            (1 to colCount) foreach {
              col => {
                if (col > 1) writer.write("|")
                writer.write(rs.string(col))
              }
            }
        }
    } //.close()
    writer.close()
  }
}

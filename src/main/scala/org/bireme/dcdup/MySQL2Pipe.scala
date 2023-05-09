/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import io.circe._
import io.circe.parser._

import java.io.BufferedWriter
import java.nio.charset.{Charset, CodingErrorAction}
import java.nio.file.{Files,Paths}
import java.sql.{DriverManager, ResultSet, Statement}

import scala.io._
import scala.util.Try

/** Exports some MySQL records fields to a piped file.
*
* author: Heitor Barbieri
* date: 20170410
*/
object MySQL2Pipe extends App {
  private def usage(): Unit = {
    System.err.println("usage: MySQL2Pipe" +
      "\n\t-host=<MySQL_Host> - MySQL host url" +
      "\n\t-user=<MySQL_User> - MySQL user" +
      "\n\t-pswd=<MySQL_Password> - MySQL password" +
      "\n\t-dbnm=<MySQL_Dbname> - MySQL database name" +
      "\n\t-sqls=<sqlFile>[,<sqlFile>,...,<sqlFile>] - sql specification files" +
      "\n\t-pipe=<pipeFile> - output piped file" +
      "\n\t[-port=<MySQL_Port>] - MySQL port" +
      "\n\t[-sqlEncoding=<sqlEncoding>] - sql file encoding. Default is utf-8" +
      "\n\t[-pipeEncoding=<pipeEncoding>] - output piped file encoding. Default is utf-8" +
      "\n\t[-jsonField=<tag>[,<tag>,...,<tag>]] - if a column element is a json element, indicates which" +
      "json elements to retrieve the content." +
      "\n\t[-repetitiveField=<name>[.<name>,...,<name>]] - the name of the fields that should be broken " +
      "into a new line when the repetitive separator symbol is found" +
      "\n\t[-repetitiveSep=<separator>] - repetitive field string separator. Default is //@//" +
      "\n\t[-jsonLangField=<jsonLangField>] - the json field that store the language indicator. If present it will be " +
        "used to suffix the id field with the language" +  // _i
      "\n\t[-idFieldName=<name>] - id field name of the mysql record]. Will be used to prefix the id with the language " +
        "if the jsonLangField is specified"
    )
    System.exit(1)
  }

  val seq = args.toSeq.filter(_.nonEmpty)
  if (seq.length < 6) usage()

 // Parse parameters
  val parameters = seq.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 2) map + ((split(0).substring(1), split(1)))
      else {
        usage()
        map
      }
  }
  val keys = parameters.keys.toSet
  if (!Set("host", "user", "pswd", "dbnm", "sqls", "pipe").forall(keys.contains)) usage()

  val host = parameters("host")
  val user = parameters("user")
  val pswd = parameters("pswd")
  val dbnm = parameters("dbnm")
  val sqlfs = parameters("sqls").trim.split(" *, *").toSet
  val pipe = parameters("pipe")
  val port = parameters.getOrElse("port", "3306")
  val sqlEncoding = parameters.getOrElse("sqlEncoding", "utf-8")
  val pipeEncoding = parameters.getOrElse("pipeEncoding", "utf-8")
  val jsonField = parameters.getOrElse("jsonField", "text,_f").trim.split(" *, *").toSet
  val repetitiveField = parameters.getOrElse("repetitiveField", "title").trim.split(" *, *").toSet
  val repetitiveSep = parameters.getOrElse("repetitiveSep", "//@//")
  val jsonLangField = parameters.getOrElse("jsonLangField", "_i")
  val idFieldName = parameters.getOrElse("idFieldName", "reference_ptr_id").toLowerCase

  //Class.forName("com.mysql.jdbc.Driver")
  Class.forName("com.mysql.cj.jdbc.Driver")

  sql2pipe(host, user, pswd, dbnm, sqlfs, pipe, port,
           sqlEncoding, pipeEncoding, jsonField, repetitiveField, repetitiveSep)

  /**
    * Convert the result of a set of sql queries into a piped file
    *
    * @param host MySQL host url
    * @param user MySQL user
    * @param pswd MySQL password
    * @param sqlfs set of files having sql statements
    * @param pipe output piped file
    * @param port MySQL host port
    * @param sqlEncoding the sql file character encoding
    * @param pipeEncoding output piped file encoding
    * @param jsonField if a column element is a json element, indicates which
    *  json elements to retrieve the content.
    * @param repetitiveFields the name of the fields that should be broken " +
    * "into a new line when the repetitive separator symbol is found"
    * @param repetitiveSep repetitive field string separator
    */
  def sql2pipe(host: String,
               user: String,
               pswd: String,
               dbnm: String,
               sqlfs: Set[String],
               pipe: String,
               port: String,
               sqlEncoding: String,
               pipeEncoding: String,
               jsonField: Set[String],
               repetitiveFields: Set[String],
               repetitiveSep: String): Unit = {
    val writer = Files.newBufferedWriter(Paths.get(pipe),
                                         Charset.forName(pipeEncoding))
    val con = DriverManager.getConnection(
                //s"jdbc:mysql://${host.trim}:3306/${dbnm.trim}",
                s"jdbc:mysql://${host.trim}:$port/${dbnm.trim}?useTimezone=true&serverTimezone=UTC&useSSL=false",
                                                                     user, pswd)
    val statement = con.createStatement()

    sqlfs.foreach {
      file => {
        println(s"+++ Converting records from [$file]")
        sqlFile2pipe(file, sqlEncoding, statement, writer, jsonField, repetitiveFields, repetitiveSep)
      }
    }
    println("+++ All records converted!")

    writer.close()
    statement.close()
    con.close()
  }

  /**
    * Convert the result of a sql query into a piped file
    *
    * @param sqlf file having the sql statement
    * @param sqlEncoding the sql file character encoding
    * @param statement the object used for executing a static SQL statement
    * @param writer the output file (piped file)
    * @param jsonField if a column element is a json element, indicates which
    *  json elements to retrieve the content.
    * @param repetitiveFields the name of the fields that should be broken " +
    * "into a new line when the repetitive separator symbol is found"
    * @param repetitiveSep repetitive field string separator
    */
  private def sqlFile2pipe(sqlf: String,
                           sqlEncoding: String,
                           statement: Statement,
                           writer: BufferedWriter,
                           jsonField: Set[String],
                           repetitiveFields: Set[String],
                           repetitiveSep: String): Unit = {
    val codec = sqlEncoding.toLowerCase match {
      case "iso8859-1" => scala.io.Codec.ISO8859
      case _           => scala.io.Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val reader = Source.fromFile(sqlf)(decoder)
    val content = reader.getLines().mkString(" ")
    reader.close()

    val rs = statement.executeQuery(content)
    val meta = rs.getMetaData
    val cols = meta.getColumnCount
    val names = (1 to cols).foldLeft[List[String]](List()) {
      case (lst, col) => lst :+ meta.getColumnName(col)
    }
    val idFieldPos =  (1 to cols).find(pos => meta.getColumnName(pos).toLowerCase.equals(idFieldName))
    var tell = 0

    while (rs.next()) {
      parseRecord(rs, names, cols, jsonField,repetitiveFields, repetitiveSep).foreach {
        line =>
          val pipe: String = suffixIdWithLang(line, idFieldPos.map(_-1))
          //println(pipe)
          writer.write(pipe)
          writer.newLine()
          if (tell % 10000 == 0) println(s"+++$tell")
          tell += 1
      }
    }
  }

  /**
  * Given a pipe line, prefix the idFieldPos element with circumflex accent '&#94;' plus the language 2 letters
    * @param str input pipe string
    * @param idFieldPos position in the pipe of the position field string
    * @return the output pipe string where the id field is suffixed with hifen plus the 2 letters language. For ex: 53498-pt
    */
  def suffixIdWithLang(str: String,
                       idFieldPos: Option[Int]): String = {
    val elems: Array[String] = str.split(" *\\| *", Integer.MAX_VALUE)
    val langIndex: Int = elems.indexWhere(_.matches("\\([a-z][a-z]\\).+"))

    if (langIndex == -1) str
    else {
      idFieldPos match {
        case Some(ifPos) =>
          val lang = elems(langIndex).substring(1, 3)

          elems.zipWithIndex.foldLeft("") {
            case (str1: String, (elem:String, idx:Int)) =>
              val prefix = if (idx == 0) str1 else str1 + "|"

              if (idx == langIndex) prefix + elem.substring(4) // index of the field that starts with the language. For ex |(pt)Meu querido...|
              else if (idx == ifPos) s"$prefix$elem^$lang"
              else prefix + elem
          }
        case None => str
      }
    }
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
      case(lst,col) =>
        val str = Try(rs.getString(col)) getOrElse "????"
//println(s"[$col]${rs.getString(col)}")
        val str2 = if (str == null) "" else str.trim()
        val str3 = if (Tools.isUtf8Encoding(str2)) str2 else "????"
        val str4 = if (str3.startsWith("[{") || str3.startsWith("{"))
                     getElement(str3, jsonField, repetitiveSep)
                   else str3
        val str5 = str4.replace("|", " ")
        val elems: List[String] = parseCell(str5, repetitiveFields(names(col - 1)), repetitiveSep)
        if (lst.isEmpty) elems
        else elems.flatMap(elem => lst.map(l => s"$l|$elem"))
    }
  }

  /** Given a cell content, it returns a list of one element (the content itself)
    * if splitLine is false or repetitiveSep symbol is not found or a list of
    * piped lines otherwise. For ex "[a][b][c//@//d][e]" will result:
    *  a|b|c|e and a|b|d|e for //@// repetitiveSeparator
    *
    * @param celContent the cell content (document field)
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
      else findElement(doc, jsonField, repetitiveSep).getOrElse(json)
    }
  }

  /** Retrieves a json document field and prefix it with the language
    *
    * @param doc Jsonobject document object
    * @param jsonField set of field names whose first content found will be retrieved
    * @param repetitiveSep symbol to be used to concatenate field content which is a listFiles
    * return Some with the field content or None otherwise
    */
  private def findElement(doc: Json,
                          jsonField: Set[String],
                          repetitiveSep: String): Option[String] = {
    if (doc.isArray) {
      doc.asArray.map(vec => vec.flatMap(json => findElement(json, jsonField, repetitiveSep)).mkString(repetitiveSep))
    } else if (doc.isObject) {
      doc.asObject.flatMap(obj => findElement(obj, jsonField, repetitiveSep))
    } else None
  }

    /** Retrieves a json document field and prefix it with the language
    *
    * @param obj Jsonobject document object
    * @param jsonField set of field names whose first content found will be retrieved
    * @param repetitiveSep symbol to be used to concatenate field content which is a listFiles
    * return Some with the field content or None otherwise
    */
  private def findElement(obj: JsonObject,
                          jsonField: Set[String],
                          repetitiveSep: String): Option[String] = {
    if (obj.isEmpty) None
    else {
      if (jsonField.isEmpty) None
      else {
        val fld = jsonField.head
        obj(fld) match {
          case Some(json) =>
            json.asString.flatMap {
              str: String =>
                if (str.isEmpty) findElement(obj, jsonField.tail, repetitiveSep)
                else {
                  obj(jsonLangField) match {
                    case Some(json2: Json) =>
                      json2.asString.map {
                        lang =>
                          val langT = lang.trim.toLowerCase
                          s"($langT)$str"
                      }
                    case None => Some(str)
                  }
                }
            }
          case None => findElement(obj, jsonField.tail, repetitiveSep)
        }
      }
    }
  }
}

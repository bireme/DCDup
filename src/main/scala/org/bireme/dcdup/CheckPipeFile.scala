/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.{BufferedWriter, FileOutputStream, IOException, OutputStreamWriter, StringReader}
import java.nio.charset.{Charset, CharsetDecoder, CharsetEncoder, CodingErrorAction}

import io.circe.{HCursor, Json, JsonObject, ParsingFailure}
import io.circe.parser._
import javax.xml.parsers.{DocumentBuilder, DocumentBuilderFactory}
import org.bireme.dcdup.CheckPipeFile.Check
import org.w3c.dom.{Document, NodeList}
import org.xml.sax.InputSource
import scalaj.http.{Http, HttpOptions}

import scala.io._
import scala.util.{Failure, Success, Try}

/** Check an input piped file is well-formed against a local Ngrams schema file or against
  * a remote schema file in a DeDup server.
  *
  * author: Heitor Barbieri
  * date: 20170416
  */
object CheckPipeFile extends App {

  case class Check(
    minFields: Int,                                // minimum number of fields with contents
    idFieldPos: Int,                               // position of the id field
    dbFieldPos: Int,                               // position of the database field
    otherFields: Map[String, (Int, Option[Int])]   // field name -> (field pos, required field pos)
  )

  private def usage(): Unit = {
    System.err.println("Check an input piped file against a remote/local Ngrams schema" +
      " file in a DeDup server." +
      "\n\nusage: CheckPipeFile" +
      "\n\t-pipe=<pipeFile> - input piped file" +
      "\n\t(" +
      "\n\t  -dedupUrl=<DeDupBaseUrl> - DeDup url service  (http://dedup.bireme.org/services)" +
      "\n\t  -schema=<schemaName> - DeDup schema name" +
      "\n\t  |" +
      "\n\t  -schema=<path>) - DeDup schema file path" +
      "\n\t)" +
      "\n\t-good=<file path> - file that contains piped lines following the schema" +
      "\n\t-bad=<file path> - file that contains piped lines that does not follow the schema" +
      "\n\t[-pipeEncoding=<encoding>] - piped file encoding. Default is utf-8" +
      "\n\t[-schemaEncoding=<schemaFileEncoding>] - NGram schema file encoding. Default is utf-8"
    )
    System.exit(1)
  }
  val seq = args.toSeq.filter(_.nonEmpty)

  if (seq.length < 4) usage()
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
  if (!Set("pipe", "schema", "good", "bad").forall(keys.contains)) usage()

  val pipe = parameters("pipe")
  val encoding = parameters.getOrElse("pipeEncoding", "utf-8").trim
  val encoding2 = if (encoding.isEmpty) "utf-8" else encoding
  val dedupUrl = parameters.get("dedupUrl")
  val schema = parameters.get("schema")
  val schemaEncoding = parameters.getOrElse("schemaEncoding", "utf-8").trim
  val schemaEncoding2 = if (schemaEncoding.isEmpty) "utf-8" else schemaEncoding
  val good = parameters("good")
  val bad = parameters("bad")

  Try {
    dedupUrl match {
      case Some(durl) =>
        schema match {
          case Some(sch) =>
            val baseUrlTrim = durl.trim
            val burl = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
            val url: String = burl + "schema/" +  sch.trim
            VerifyPipeFile.checkRemote(pipe, encoding2, url, good, bad)
          case None => new IllegalArgumentException("use 'dedupUrl + schema' or 'schemaPath'")
        }
      case None =>
        schema match {
          case Some(spath) => VerifyPipeFile.checkLocal(pipe, encoding2, spath, good, bad, schemaEncoding2)
          case None => new IllegalArgumentException("use 'dedupUrl + schema' or 'schemaPath'")
        }
    }
  } match {
    case Success((goodDocs, badDocs)) =>
      println(s"Properly formatted lines: $goodDocs")
      println(s"Incorrectly formatted lines : $badDocs")
    case Success(e) => throw new IOException(e.toString)
    case Failure(e) => throw e
  }
}

/** Check an input piped file against a local Ngrams schema file or against
  * a remote schema file in a DeDup server.
  */
object VerifyPipeFile {

  /**
    * Check an input piped file against a local DeDup schema file
    *
    * @param pipe - input piped file to be checked
    * @param encoding - piped file character encoding
    * @param schemaFile - Path to the DeDup schema file.
    * @param good - path of the temporary file having lines that follow the schema
    * @param bad - path of the temporary file having lines that do not follow the schema
    * @param schemaFileEncoding - schema file encoding
    * @return (number of good lines, number of bad lines)
    */
  def checkLocal(pipe: String,
                 encoding: String,
                 schemaFile: String,
                 good: String,
                 bad: String,
                 schemaFileEncoding: String = "utf-8"): (Int, Int) = {
    val codAction: CodingErrorAction = CodingErrorAction.REPLACE
    val encoder1: CharsetEncoder = Charset.forName("utf-8").newEncoder()
                                   .onMalformedInput(codAction).onUnmappableCharacter(codAction)
    val encoder2: CharsetEncoder = Charset.forName("utf-8").newEncoder()
                                   .onMalformedInput(codAction).onUnmappableCharacter(codAction)
    val decoder: CharsetDecoder = Charset.forName(encoding).newDecoder()
                                  .onMalformedInput(codAction).onUnmappableCharacter(codAction)
    val reader: BufferedSource = Source.fromFile(pipe)(decoder)
    val lines: Iterator[String] = reader.getLines()
    val source: BufferedSource = Source.fromFile(schemaFile, schemaFileEncoding)
    val schema: String = source.getLines().mkString(" ")
    val goodWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(
                                                        new FileOutputStream(good), encoder1))
    val badWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(
                                                       new FileOutputStream(bad), encoder2))
    val (goodDocs, badDocs) = checkRaw(lines, schema, isJson = false, goodWriter, badWriter)

    reader.close()
    source.close()
    goodWriter.close()
    badWriter.close()

    (goodDocs,badDocs)
  }

  /**
    * Check an input piped file against a remote DeDup schema file
    *
    * @param pipe - input piped file to be checked
    * @param encoding - piped file character encoding
    * @param schemaUrl - url of the DeDup schema page. For example: http://dedup.bireme.org/services/schema/LILACS_Sas_Seven
    * @param good - path of the temporary file having lines that follow the schema
    * @param bad - path of the temporary file having lines that do not follow the schema
    * @return (number of good lines, number of bad lines)
    */
  def checkRemote(pipe: String,
                  encoding: String,
                  schemaUrl: String,
                  good: String,
                  bad: String): (Int, Int) = {
    val codAction: CodingErrorAction = CodingErrorAction.REPLACE
    val encoder1: CharsetEncoder = Charset.forName("utf-8").newEncoder()
      .onMalformedInput(codAction).onUnmappableCharacter(codAction)
    val encoder2: CharsetEncoder = Charset.forName("utf-8").newEncoder()
      .onMalformedInput(codAction).onUnmappableCharacter(codAction)
    val decoder: CharsetDecoder = Charset.forName(encoding).newDecoder()
      .onMalformedInput(codAction).onUnmappableCharacter(codAction)
    val reader: BufferedSource = Source.fromFile(pipe)(decoder)
    val lines: Iterator[String] = reader.getLines()
    val schema: String = Http(schemaUrl).option(HttpOptions.followRedirects(true))
                         .charset("utf-8").asString.body
    val goodWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(good), encoder1))
    val badWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(bad), encoder2))
    val (goodDocs, badDocs) = checkRaw(lines, schema, isJson = true, goodWriter, badWriter)

    reader.close()
    goodWriter.close()
    badWriter.close()

    (goodDocs,badDocs)
  }

  /**
    * Check an input piped file against a DeDup schema file
    *
    * @param lines - input piped file line iterator
    * @param schema - DeDup schema content string
    * @param isJson - indicate if the schema format is json (true) or xml (false)
    * @param goodWriter - writer to the temporary file having lines that follow the schema
    * @param badWriter - writer to the temporary file having lines that do not follow the schema
    * @return (number of good lines, number of bad lines)
    */
  private def checkRaw(lines: Iterator[String],
                       schema: String,
                       isJson: Boolean,
                       goodWriter: BufferedWriter,
                       badWriter: BufferedWriter): (Int,Int) = {
    val optCheck: Option[Check] = if (isJson) parseSchemaJson(schema) else parseSchemaXml(schema)

    optCheck match {
      case Some(check) =>
        lines.foldLeft[(Int, Int)](0, 0) {
          case ((good, bad), line) =>
            checkLine(check, line) match {
              case Some(errMess) =>
                badWriter.write(s"$line [$errMess]\n")
                (good, bad + 1)
              case None =>
                goodWriter.write(line + "\n")
                (good + 1, bad)
            }
        }
      case None => (0, 0)
    }
  }

  /**
    * Parse a Json DeDup schema string and convert it into a Check object
    *
    * @param schema DeDup schema content string (json format)
    * @return the Check case class
    */
  private def parseSchemaJson(schema: String): Option[Check] = {
    Try {
      parse(schema) match {
        case Right(doc: Json) =>
          getFieldsJson(doc).map {
            fields: Map[String, Map[String, Option[String]]] =>
              val fields2: Map[String, (Int, Option[Int])] = getFieldsJson(fields)

              Check(
                getMinFieldsJson(doc).get,
                getIdPosJson(fields2).get,
                getDbPosJson(fields2).get,
                fields2
              )
          }
        case Left(failure: ParsingFailure) => throw failure.underlying
      }
    } match {
      case Success(check) => check
      case Failure(exception) =>
        println(s"parse ERROR: ${exception.getMessage}")
        None
    }
  }

  /**
    * Parse a xml DeDup schema string and convert it into a map
    *
    * @param schema DeDup schema content string (xml format)
    * @return the Check case class
    */
  private def parseSchemaXml(schema: String): Option[Check] = {
    Try {
      val docFactory: DocumentBuilderFactory = DocumentBuilderFactory.newInstance()
      val docBuilder: DocumentBuilder = docFactory.newDocumentBuilder()
      val doc: Document = docBuilder.parse(new InputSource(new StringReader(schema)))
      val fields: Map[String, (Int, Option[Int])] = getFieldsXml(doc)

      Check(
        getMinFieldsXml(doc).get,
        getIdPosXml(doc).get,
        getDbPosXml(doc).get,
        fields
      )
    } match {
      case Success(check) => Some(check)
      case Failure(exception) =>
        println(s"parse ERROR: ${exception.getMessage}")
        None
    }
  }

  private def getMinFieldsJson(doc: Json): Option[Int] = {
    val cursor: HCursor = doc.hcursor
    val x1: Option[Iterable[Json]] = cursor.downField("score").values
    val x2: Option[Iterable[JsonObject]] = x1.map(_.flatMap(_.asObject))
    val x3: Option[Iterable[Map[String, Json]]] = x2.map(_.map(_.toMap))
    val x4: Option[Iterable[Json]] = x3.map(_.flatMap(_.get("minFields")))
    val x5: Option[Iterable[String]] = x4.map(_.flatMap(_.asString))
    val x6: Option[Iterable[Int]] = x5.map(_.map(_.toInt))

    x6.map(_.min)
  }

  private def getMinFieldsXml(doc: Document): Option[Int] = {
    val list = doc.getElementsByTagName("score")
    val result: Int = (0 until list.getLength).foldLeft(Integer.MAX_VALUE) {
      case (min, idx) =>
        Option(list.item(idx).getAttributes) match {
          case Some(att) =>
            Option(att.getNamedItem("minFields")) match {
              case Some(minf) =>
                  val mf = minf.getTextContent.toInt
                  Math.min(min, mf)
              case None => Integer.MAX_VALUE
            }
          case None => Integer.MAX_VALUE
        }
    }
    if (result == Integer.MAX_VALUE) None else Some(result)
  }

  /**
    * @param doc Json param element
    * @return the 'pos' value of the param whose name element is 'id'
    */
  private def getIdPosJson(doc: Map[String, (Int, Option[Int])]): Option[Int] = {
    doc.get("id").map(_._1)
  }

  /**
    * @param doc Xml param element
    * @return the 'pos' value of the param whose name element is 'id'
    */
  private def getIdPosXml(doc: Document): Option[Int] = {
    val list = doc.getElementsByTagName("idField")
    if (list.getLength == 0) None
    else {
      Option(list.item(0).getAttributes).flatMap {
        att => Option(att.getNamedItem("pos")).map(_.getTextContent.toInt)
      }
    }
  }

  /**
    * @param doc Json param element
    * @return the 'pos' value of the param whose name element is 'database'
    */
  private def getDbPosJson(doc: Map[String, (Int, Option[Int])]): Option[Int] = {
    doc.get("database").map(_._1)
  }

  /**
    * @param doc Xml param element
    * @return the 'pos' value of the param whose name element is 'database'
    */
  private def getDbPosXml(doc: Document): Option[Int] = {
    val list: NodeList = doc.getElementsByTagName("databaseField")
    if (list.getLength == 0) None
    else {
      Option(list.item(0).getAttributes).flatMap {
        att => Option(att.getNamedItem("pos")).map(_.getTextContent.toInt)
      }
    }
  }

  /**
    * Convert a json document into a map of its 'params' elements
    * @param doc input json document
    * @return map of 'name' field -> json element (params(x) element)
    */
  private def getFieldsJson(doc: Json): Option[Map[String, Map[String, Option[String]]]] = {
    val cursor: HCursor = doc.hcursor
    val x1: Option[Iterable[Json]] = cursor.downField("params").values
    val x2: Option[Seq[Json]] = x1.map(_.toSeq)
    val x3: Option[Seq[JsonObject]] = x2.map(_.flatMap(_.asObject))
    val x4: Option[Seq[Map[String, Json]]] = x3.map(_.map(_.toMap))
    val x5: Option[Seq[Map[String, Option[String]]]] = x4.map {
      seq: Seq[Map[String, Json]] => seq.map {
        map =>
          map.map {
            kv =>
              val vl: String = {
                if (kv._2.isNumber) kv._2.asNumber.map(_.toString).getOrElse("").trim
                else kv._2.asString.getOrElse("").trim
              }
              val value: Option[String] = if (vl.isEmpty) None else Some(vl)
              kv._1 -> value
          }
      }
    }
    x5.map {
      seq => seq.foldLeft(Map[String, Map[String, Option[String]]]()) {
        case (map:Map[String, Map[String, Option[String]]], auxmap:Map[String, Option[String]]) =>
          auxmap.get("name") match {
            case Some(oname) =>
              oname match {
                case Some(name) => map + (name -> auxmap)
                case None => map
              }
            case None => map
          }
      }
    }
  }

  /**
    * Convert a map of its 'params' elements into a map of 'name' field -> ('pos', 'requiredField' pos)
    * @param doc input json document
    * @return map of 'name' field -> ('pos', 'requiredField' pos)
    */
  private def getFieldsJson(doc: Map[String, Map[String, Option[String]]]): Map[String, (Int, Option[Int])] = {
    doc.map {
      kv =>
        val mp: Map[String, Option[String]] = kv._2
        val pos: Int = mp("pos").get.toInt
        val reqField: Option[Int] = mp.get("requiredField").flatten.flatMap {
          reqFldName: String =>
            doc.get(reqFldName).flatMap(_.get("pos").flatten.map(_.toInt))
        }
        kv._1 -> (pos, reqField)
    }
  }

  /**
    *
    * @param doc input xml representation
    * @return a map of (fieldName -> (fieldPosition, reqFieldPosition))
    */
  private def getFieldsXml(doc: Document): Map[String, (Int, Option[Int])] = {
    val map: Map[String, (Int, Option[String])] =
      Seq("idxNGramField", "nGramField", "exactField", "regExpField").foldLeft(Map[String, (Int, Option[String])]()) {
        case (map1, fname) =>
          val list = doc.getElementsByTagName(fname)

          (0 until list.getLength).foldLeft(map1) {
            case (map2, idx) => Option(list.item(idx).getAttributes).flatMap {
              att =>
                Option(att.getNamedItem("pos")).flatMap {
                  pos =>
                    Option(att.getNamedItem("name")).map {
                      name =>
                        val tuple: (Int, Option[String]) =
                          (pos.getTextContent.toInt, Option(att.getNamedItem("requiredField")).map(_.getTextContent))
                        map2 + (name.getTextContent -> tuple)
                    }
                }
            }.get
          }
      }
    map.map {
      case (key:String, (pos1:Int, opt:Option[String])) =>
        opt match {
          case Some(reqField) => key -> (pos1, Some(map(reqField)._1))
          case None => key -> (pos1, None)
        }
    }
  }

  /*private def checkLine0(check: Check,
                         line: String): Boolean = {
    val fields: Array[String] = line.trim.split(" *\\| *", 100)
    val nonEmptyFields: Int = fields.foldLeft(0) {
      case (tot, fld) => if (fld.trim.nonEmpty) tot + 1 else tot
    }
    if(false) {
      val x1 = (nonEmptyFields - 3) >= check.minFields
      val x2 = fields(check.idFieldPos).nonEmpty
      val x3 = fields(check.dbFieldPos).nonEmpty
      val x4 = check.otherFields.forall { // Check each other field condition
        field =>
          val y0 = field._2._1 < fields.length
          if (y0) {
            val y1 = fields(field._2._1).isEmpty
            val y2 = field._2._2.forall {
              reqField => fields(reqField).nonEmpty
            }
            y1 || y2
          } else false
      }
      val x5 = x1 && x2 && x3 && x4
      println(x5)
    }
    (nonEmptyFields - 3) >= check.minFields  &&    // Check if there are a minimum number of non empty fields except (id, database, title)
    fields(check.idFieldPos).nonEmpty &&           // Check if id field is present
    fields(check.dbFieldPos).nonEmpty &&           // Check if database field is present
    check.otherFields.forall {                     // Check each other field condition
      field =>
        field._2._1 < fields.length &&
          (fields(field._2._1).isEmpty ||           // Check if the field is present or required field is not
           field._2._2.forall(reqField => fields(reqField).nonEmpty))
    }
  }*/

  /**
    * Check if a piped line follows the schema
    *
    * @param check the schema information
    * @param line the line to be checked
    * @return the cause why the line broke a schema's rule
    */
  private def checkLine(check: Check,
                        line: String): Option[String] = {
    val fields: Array[String] = line.trim.split(" *\\| *", 100)
    val nonEmptyFields: Int = fields.foldLeft(0) {
      case (tot, fld) => if (fld.trim.nonEmpty) tot + 1 else tot
    }

    if ((nonEmptyFields - 2) < check.minFields) Some(s"number of fields are less than ${check.minFields}")
    else if (fields(check.idFieldPos).isEmpty) Some("missing id field")
    else if (fields(check.dbFieldPos).isEmpty) Some("missing database field")
    else check.otherFields.foldLeft[Option[String]](None) { // Check each other field condition
      case (opt, field) =>
        if (opt.isEmpty) {
            if (field._2._1 >= fields.length) Some(s"invalid field pos = ${field._2._1}")
            else if (fields(field._2._1).nonEmpty && field._2._2.exists(fields(_).isEmpty))
              Some(s"missing required field pos=${field._2._1}")
            else None
        } else None
    }
  }
}

package org.bireme.dcdup

import org.apache.commons.csv.{CSVFormat, CSVPrinter, CSVRecord}

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import java.nio.charset.CodingErrorAction
import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData}
import scala.io.{BufferedSource, Codec, Source}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

object EnrichDupReport extends App {
  private def usage(): Unit = {
    System.err.println("Take the ids of the duplicated documents (from the duplicated documents report) and use then to extract more fields from MySQL database")
    System.err.println("The original fields and the extracted ones are put in a 'enriched duplicated documents report")
    System.err.println("usage: EnrichDupReport <options>\noptions:" +
      "\n\t-inputDupReport=<path> - pipe file having duplicated documents (output of the deduplication process)" +
      "\n\t-enrichedDupReport=<path> - pipe file generated from inputDupReport plus adicional fields extracted with a sql" +
      "\n\t-sqlFile=<path> - file having the sql expression to retrieve the adicional fields" +
      "\n\t-host=<MySQL_Name> - MySQL host name" +
      "\n\t-user=<MySQL_User> - MySQL user" +
      "\n\t-pswd=<MySQL_Password> - MySQL password" +
      "\n\t-dbnm=<MySQL_Dbname> - MySQL database name" +
      "\n\t[-idPos1=<pos>] - position of the first document id in the inputDupReport. Positions start with 0. Default value is 4." +
      "\n\t[-idPos2=<pos>] - position of the second document id in the inputDupReport. Positions start with 0. Default value is 5." +
      "\n\t[--isoSql] - if present, indicate the sql file is encoded with ISO-8859-1. Default is UTF-8." +
      "\n\t[--deleteInputReport] - if present, after generating the enriched report, it deletes the inputDupReport"
    )
    System.exit(1)
  }
  if (args.length < 3) usage()

  private val parameters: Map[String, String] = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + (split(0).substring(2) -> "")
      else map + (split(0).substring(1) -> split(1))
  }
  private val idPos1: Int = parameters.getOrElse("idPos1", "4").toInt
  private val idPos2: Int = parameters.getOrElse("idPos2", "5").toInt
  private val port: Int = parameters.getOrElse("port", "3306").toInt
  private val isoSql: Boolean = parameters.contains("isoSql")
  private val deleteInputReport: Boolean = parameters.contains("deleteInputReport")

  enrich(parameters("inputDupReport"), idPos1, idPos2, parameters("enrichedDupReport"), parameters("host"), port, parameters("dbnm"),
    parameters("user"), parameters("pswd"), parameters("sqlFile"), isoSql, deleteInputReport) match {
    case Success(_) =>
      println("Enriched report successfully created!")
      System.exit(0)
    case Failure(exception) =>
      println(s"Enriched report creation error: ${exception.toString}")
      System.exit(1)
  }

  def enrich(inputDupReport: String,
             idPos1: Int,
             idPos2: Int,
             enrichedDupReport: String,
             host: String,
             port: Int,
             dbName: String,
             user: String,
             password: String,
             sqlFile: String,
             isoSql: Boolean = false,
             deleteInputDupReport: Boolean = true): Try[Unit] = {
    Try {
      val reader: BufferedReader = new BufferedReader(new FileReader(inputDupReport))
      val writer: BufferedWriter = new BufferedWriter(new FileWriter(enrichedDupReport))
      val csvFormat: CSVFormat = CSVFormat.Builder.create().setDelimiter('|').setIgnoreEmptyLines(true).build()
      val iter: Iterator[CSVRecord] = csvFormat.parse(reader).iterator().asScala
      val printer: CSVPrinter = new CSVPrinter(writer, csvFormat)

      val codec: Codec = if (isoSql) scala.io.Codec.ISO8859 else scala.io.Codec.UTF8
      val codAction = CodingErrorAction.REPLACE
      val decoder = codec.decoder.onMalformedInput(codAction)
      val sqlReader: BufferedSource = Source.fromFile(sqlFile)(decoder)
      val sqlContent: String = sqlReader.getLines().mkString("\n")
      val con: Connection = DriverManager.getConnection(s"jdbc:mysql://${host.trim}:$port/${dbName.trim}?useTimezone=true&serverTimezone=UTC&useSSL=false", user, password)

      enrich(iter, idPos1, idPos2, printer, con, sqlContent)
      sqlReader.close()
      con.close()
      printer.close()
      reader.close()
      if (deleteInputDupReport) new File(inputDupReport).delete()
    }
  }

  def enrich(iter: Iterator[CSVRecord],
             idPos1: Int,
             idPos2: Int,
             printer: CSVPrinter,
             con: Connection,
             sqlContent: String): Unit = {
    val statement = con.createStatement()
    val rs: ResultSet = statement.executeQuery(sqlContent)
    val meta: ResultSetMetaData = rs.getMetaData
    val cols: Int = meta.getColumnCount
    val names: Seq[String] = (1 to cols).foldLeft[List[String]](List()) {
      case (lst, col) => lst :+ meta.getColumnName(col)
    }
    //val idFieldPos =  (1to cols).find(pos => meta.getColumnName(pos).toLowerCase.equals(idFieldName))
    var tell = 0

    /*while (rs.next()) {
      parseRecord
COMO CONSULTAR:

Diretamente em um dos sites abaixo:(rs, names, cols, jsonField, repetitiveFields, repetitiveSep).foreach {
        line =>
          val pipe: String = suffixIdWithLang(line, idFieldPos.map(_-1))
          //println(pipe)
          writer.write(pipe)
          writer.newLine()
          if (tell % 10000 == 0) println(s"+++$tell")
          tell += 1
      }
    }*/
  }
}

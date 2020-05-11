package org.bireme.dcdup

import java.io.{BufferedWriter, File, FileInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}

import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

/** Export xml document fields to a piped file according to a conversion file.
*
* author: Heitor Barbieri
* date: 20191009
*/
object Xml2Pipe extends App {
  private def usage(): Unit = {
    System.err.println("usage: Xml2Pipe <options>")
    System.err.println("\toptions:")
    System.err.println("\t-dir=<dir> - directory of input xml files")
    System.err.println("\t-inPattern=<xml_pattern> - regular expression filter of the input xml files")
    System.err.println("\t-outDir=<dir> - output directory")
    System.err.println("\t-conv=<conversionFile> - conversion table file (one line for each conversion) with the following the format:")
    System.err.println("\t\t<document type>|<conversion table path - conv paremeter>")
    System.err.println("\t\twhere the conversion table file (one line for each conversion) has the following the format:")
    System.err.println("\t\t<document xml element>|<position in the pipe line>")
    System.err.println("\t[-exPattern=<xml_pattern>] - regular expression filter of the excluded xml files")
    System.exit(1)
  }

  if (args.length < 4) usage()

  // Parse parameters
  val parameters: Map[String, String] = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 2) map + ((split(0).substring(1), split(1)))
      else {
        usage()
        map
      }
  }

  val repSeparator: String = "//@//"  // Separator string of repetitive field
  val dir: String = parameters("dir")
  val inPattern: String = parameters("inPattern")
  val outDir: String = parameters("outDir")
  val conv: String = parameters("conv")
  val exPattern: Option[String] = parameters.get("exPattern")

  val convMap: Map[String, (Int, Map[String, Int])] = parseConvFile1(conv).foldLeft(Map[String, (Int, Map[String, Int])]()) {
    case (map, (xmlTypeElem, filePath)) =>
      val conv2: Map[String, Int] = parseConvFile2(filePath)
      val maxPos: Int = conv2.values.max
      map + (xmlTypeElem.toLowerCase() -> (maxPos, parseConvFile2(filePath)))
  }

  convert(dir, inPattern, convMap, outDir, exPattern)

  /**
    * Parse a association file between xml document type and conversion (field,pos) file
    * @param convFile the input file
    * @return (xml field 'type' content -> associated conversion file (schema like)
    */
  private def parseConvFile1(convFile: String): Map[String, String] = {
    val src = Source.fromFile(convFile, "utf-8")

    val map: Map[String, String] = src.getLines.foldLeft(Map[String, String]()) {
      case (map1, line) =>
        val split = line.trim.split(" *\\| *", 2)
        if (split.length > 1) map1 + (split(0) -> split(1))
        else map1
    }

    src.close()
    map
  }

  /**
    * Convert between a xml document field into the pipe position
    * @param convFile the conversion file
    * @return xml tag -> pipe position
    */
  private def parseConvFile2(convFile: String): Map[String, Int] = {
    val src = Source.fromFile(convFile, "utf-8")

    val map: Map[String, Int] = src.getLines.foldLeft(Map[String, Int]()) {
      case (map1, line) =>
        val split = line.trim.split(" *\\| *", 2)
        if (split.length > 1) map1 + (split(0) -> split(1).toInt)
        else map1
    }

    src.close()
    map
  }

  def convert(dir: String,
              inPattern: String,
              conv: Map[String, (Int, Map[String, Int])],
              outDir: String,
              exPattern: Option[String]): Unit = {
    val inFileFilter: String = inPattern.trim
    val exFileFilter: String = exPattern.getOrElse("").trim
    val dirFile = new File(dir)
    if (!dirFile.isDirectory) throw new IOException(s"directory expected [$dir]")

    val writers: mutable.Map[String, BufferedWriter] = mutable.Map[String, BufferedWriter]()
    val path: Path = Paths.get(outDir)
    Files.createDirectories(path)

    dirFile.listFiles().foreach {
      file =>
        val fname: String = file.getName
        if (file.isFile && fname.matches(inFileFilter) && !fname.matches(exFileFilter))
          convertFile(file, conv, writers, outDir)
    }
    println("Conversion finished")
  }

  def convertFile(file: File,
                  conv: Map[String, (Int, Map[String, Int])],
                  writers: mutable.Map[String, BufferedWriter],
                  outDir: String): Unit = {
    val fname: String = file.getName

    System.out.print(s"Converting file [$fname]... ")
    System.out.flush()

    Try {
      val is: FileInputStream = new FileInputStream(file)
      val factory: XMLInputFactory = XMLInputFactory.newInstance
      val parser: XMLStreamReader = factory.createXMLStreamReader(is)

      val fields: Set[String] = conv.values.map(_._2).foldLeft(Set[String]()) {
        case (set, map) => set ++ map.keys
      } + "type"

      if (getAddElement(parser)) getDocuments(fields, parser).
        foreach(writeDoc(_, conv, writers, outDir, fname))
      is.close()
      writers.values.foreach(_.close())
    } match {
      case Success(_) => println("OK")
      case Failure(exception) => println(s"FAILED [${exception.getMessage}]")
    }
  }

  private def writeDoc(doc: Map[String, List[String]],
                       conv: Map[String, (Int, Map[String, Int])],
                       writers: mutable.Map[String, BufferedWriter],
                       outDir: String,
                       fname: String): Unit = {
    val dbase: String = doc.getOrElse("db", List("unknown")).mkString(repSeparator)
    val id: String = doc.getOrElse("id", List("unknown")).mkString(repSeparator)

    doc.get("type") match {
      case Some(tp) =>
        if (tp.isEmpty) println(s"Skipping document file=$fname db=$dbase id=$id - unknown document type.")
        else {
          val docType = tp.head
          conv.get(docType) match {
            case Some((maxPos, conv1)) =>
              getWriter(docType, writers, outDir, fname) match {
                case Some(wr) =>
                  //println(s"==> type=[$docType] file=$fname db=$dbase id=$id")
                  writeDocument(doc, conv1, maxPos, wr)
                case None => println(s"Skipping document file=$fname db=$dbase id=$id - output file creation error.")
              }
            case None => println(s"Skipping document file=$fname db=$dbase id=$id type=$docType - unknown conversion table.")
          }
        }
      case None => println(s"Skipping document file=$fname db=$dbase id=$id - unknown document type.")
    }
  }

  private def getWriter(docType: String,
                        writers: mutable.Map[String, BufferedWriter],
                        outDir: String,
                        fname: String): Option[BufferedWriter] = {
    Try {
      writers.get(docType) match {
        case Some(wr) => wr
        case None =>
          val oFile: File = new File(outDir, Tools.changeFileExtension(fname, s"${docType}_pipe"))
          val writer1: BufferedWriter = Files.newBufferedWriter(oFile.toPath, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
          writers += (docType -> writer1)
          writer1
      }
    }.toOption
  }

  private def writeDocument(doc: Map[String, List[String]],
                            conv: Map[String, Int],
                            maxPos: Int,
                            writer: BufferedWriter): Unit = {
    val array: Array[String] = List.fill(maxPos + 1)("").toArray

    doc foreach {
      case (k,v) =>
        if (!k.equals("type")) {
          conv.get(k) match {
            case Some(pos) => array(pos) = v.mkString(repSeparator)
            case None => println(s"ERROR: conversion to element [$k] not found.")
          }
        }
    }
    writer.write(array.mkString("|"))
    writer.newLine()
  }

  private def getAddElement(parser: XMLStreamReader): Boolean = {
    getElement(XMLStreamConstants.START_ELEMENT, parser) match {
      case Some(name) => name.equals("add")
      case None => false
    }
  }

private def getDocuments(fields: Set[String],
                           parser: XMLStreamReader,
                           first: Boolean = true): LazyList[Map[String, List[String]]] = {
    getNextDoc(fields, parser, first) match {
      case Some(doc) => doc #:: getDocuments(fields, parser, first = false)
      case None => LazyList.empty[Map[String, List[String]]]
    }
  }

  /* Before scala 2.13
  private def getDocuments(fields: Set[String],
                           parser: XMLStreamReader,
                           first: Boolean = true): Stream[Map[String, List[String]]] = {
    getNextDoc(fields, parser, first) match {
      case Some(doc) => doc #:: getDocuments(fields, parser, first = false)
      case None => Stream.Empty
    }
  }
*/

  private def getNextDoc(fields: Set[String],
                         parser: XMLStreamReader,
                         first : Boolean): Option[Map[String, List[String]]] = {
    if (first) {
      getElement(XMLStreamConstants.START_ELEMENT, parser).flatMap {
        name =>
          if (name.equals("doc")) getFields(fields, parser, None)
          else None
      }
    } else getFields(fields, parser, None)
  }

  @scala.annotation.tailrec
  private def getFields(fields: Set[String],
                        parser: XMLStreamReader,
                        aux: Option[Map[String, List[String]]]): Option[Map[String, List[String]]] = {
    getElement(XMLStreamConstants.START_ELEMENT, parser) match {
      case Some(name) =>
        name match {
          case "field" =>
            val fldName = parser.getAttributeValue(0)
            if (fields contains fldName) {
              val fldTxt = parser.getElementText
              val aux2: Map[String, List[String]] = aux.getOrElse(Map[String, List[String]]())
              val lst: List[String] = aux2.getOrElse(fldName, List[String]()) :+ fldTxt
              getFields(fields, parser, Some(aux2 + (fldName -> lst)))
            } else getFields(fields, parser, aux)
          case _ => aux
        }
      case None => aux
    }
  }

  @scala.annotation.tailrec
  private def getElement(elemType: Int,
                         parser: XMLStreamReader): Option[String] = {
    if (parser.hasNext) {
      val next: Int = parser.next()
      if (elemType == next) Some(parser.getLocalName)
      else getElement(elemType, parser)
    } else {
      None
    }
  }
}

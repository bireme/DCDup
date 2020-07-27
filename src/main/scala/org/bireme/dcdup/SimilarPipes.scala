/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.{BufferedWriter, File, IOException}
import java.nio.charset.Charset
import java.nio.file.{Files, StandardOpenOption}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

/**
  * Given a pipe file, creates two other files: the one whose lines follow a DeDup schema and the other that don't. From
  * the one that follows, creates another ones where each one, group pipe lines that contain (i.e. are not empty) elements
  * in the same position.
  */
object SimilarPipes extends App {
  private def usage(): Unit = {
    System.err.println("usage: SimilarPipes <options>" +
      "\nOptions:" +
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
  val pipeEncoding = parameters.getOrElse("pipeEncoding", "utf-8").trim
  val pipeEncoding2 = if (pipeEncoding.isEmpty) "utf-8" else pipeEncoding
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
            VerifyPipeFile.checkRemote(pipe, pipeEncoding2, url, good, bad)
          case None => new IllegalArgumentException("use 'dedupUrl + schema' or 'schemaPath'")
        }
      case None =>
        schema match {
          case Some(spath) => VerifyPipeFile.checkLocal(pipe, pipeEncoding2, spath, good, bad, schemaEncoding2)
          case None => new IllegalArgumentException("use 'dedupUrl + schema' or 'schemaPath'")
        }
    }
  } match {
    case Success((goodDocs, badDocs)) =>
      println(s"Properly formatted lines: $goodDocs")
      println(s"Incorrectly formatted lines : $badDocs")
      print("Creating similar pipe files ...")
      createPipes(pipe) match {
        case Success(_) => println(" OK")
        case Failure(e) => throw e
      }
    case Success(e) => throw new IOException(e.toString)
    case Failure(e) => throw e
  }

  /**
    * Create pipe files that group lines that contains elements in the same positions
    * @param inPipe input pipe file
    * @return nothing
    */
  def createPipes(inPipe: String): Try[Unit] = {
    val inPipe2 = inPipe.trim
    require(inPipe2.nonEmpty)

    Try {
      val lastDot = inPipe2.lastIndexOf('.')
      val (fname, extension) =  if (lastDot == -1) (inPipe2, "")
                                else (inPipe2.substring(0, lastDot), inPipe2.substring(lastDot))
      val src: BufferedSource = Source.fromFile(inPipe2)
      val iter = src.getLines()
      val writers = mutable.Map[String, BufferedWriter]()

      iter.foreach(createPipe(fname, extension, _, writers))

      writers.values.foreach(_.flush())
      writers.values.foreach(_.close())
      src.close()
    }
  }

  /**
    *
    * @param fname the input file name
    * @param extension the input file name extension
    * @param line the pipe line that will be saved in a pipe file
    * @param writers a map of string (created by getFileName) -> BufferedWriter
    */
  private def createPipe(fname: String,
                         extension: String,
                         line: String,
                         writers: mutable.Map[String, BufferedWriter]): Unit = {
    val split: Array[String] = line.split("\\|", Integer.MAX_VALUE).map(_.trim)
    assert(split.length > 2)
    val suffix: String = getFileName(split)
    val writer: BufferedWriter = writers.get(suffix) match {
      case Some(wtr) => wtr
      case None =>
        val wtr = Files.newBufferedWriter(new File(fname + "_" + suffix + extension).toPath,
                                         Charset.forName("utf-8"),
                                         StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        writers.addOne(suffix -> wtr)
        wtr
    }
    writer.write(line)
    writer.newLine()
  }

  /**
    * Given a pipe line, create a string having ones in the positions of the pipe that are not empty and zeros in the
    * positions that are empty
    * @param split the input pipe line
    * @return the string that are created
    */
  private def getFileName(split: Array[String]): String = {
    split.foldLeft("") {
      case (str, item) => str + (if (item.isEmpty) "0" else "1")
    }
  }
}

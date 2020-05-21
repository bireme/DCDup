/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.File

import br.bireme.ngrams.{NGIndex, NGSchema, NGrams}

import scala.io.Source

/**
  * Create a local Lucene index from a input piped file
  *
  * author: Heitor Barbieri
  */
object Pipe2Lucene extends App {
  private def usage(): Unit = {
    System.err.println("usage: Pipe2Lucene" +
      "\n\t-index=<indexPath> - NGram's Lucene index path" +
      "\n\t-schema=<schemaFile> - NGram schema file" +
      "\n\t-pipe=<pipeFile> - pipe file" +
      "\n\t[-schemaEncoding=<schemaFileEncoding>] - NGram schema file encoding. Default is utf-8" +
      "\n\t[-pipeEncoding=<pipeEncoding>] - pipe file encoding. Default is utf-8" +
      "\n\t[--append] - append documents to an existing Lucene index"
    )
    System.exit(1)
  }
  val seq = args.toSeq.filter(_.nonEmpty)
  if (seq.length < 3) usage()

  val parameters = seq.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  val index = parameters("index")
  val schema = parameters("schema")
  val pipe = parameters("pipe")
  val schemaEncoding = parameters.getOrElse("schemaEncoding", "utf-8")
  val pipeEncoding = parameters.getOrElse("pipeEncoding", "utf-8")
  val append = parameters.contains("append")
  val p2l = new Pipe2Lucene()

  p2l.convertToLucene(index, schema, schemaEncoding, pipe, pipeEncoding, append)
}

class Pipe2Lucene {
  def convertToLucene(indexPath: String,
                      schemaFile: String,
                      schemaEncoding: String,
                      pipeFile: String,
                      pipeEncoding: String,
                      append: Boolean): Unit = {

    val index = new NGIndex(indexPath, indexPath, false)
    val writer = index.getIndexWriter
    if (!append) {
      writer.deleteAll()
      writer.commit()
    }
    // Verifying pipe file integrity
    println(s"\nVerifying pipe file integrity [$pipeFile]")
    val goodFileName = File.createTempFile("good", "").getPath
    val badFileName = File.createTempFile("bad", "").getPath
    val (good,bad) = VerifyPipeFile.checkLocal(pipeFile, pipeEncoding, schemaFile, goodFileName, badFileName,
                                               schemaEncoding)
    println(s"Using $good documents")
    if (bad > 0) println(s"Skipping $bad documents. See file: $badFileName\n")

    val schema = new NGSchema("schema", schemaFile, schemaEncoding)
    val reader = Source.fromFile(goodFileName, "utf-8")

    reader.getLines.zipWithIndex.foreach {
      case (line,idx) =>
        if (idx % 10000 == 0) println(s"+++$idx")
        try {
          NGrams.indexDocument(index, writer, schema, line, false, false)
        } catch {
          case ex:Exception =>
            Console.err.println(s"Skipping line [$line] => ${ex.getMessage}")
        }
    }

    writer.commit()
    print("Optimizing index ...")
    writer.forceMerge(1) // optimize index
    println("OK")
    writer.close()
    reader.close()
  }
}

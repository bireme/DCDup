/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import br.bireme.ngrams.{NGrams,NGIndex,NGSchema}

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}

/**
  * Creates a local Lucene index from a input piped file
  *
  * author: Heitor Barbieri
  */
object Pipe2Lucene extends App {
  private def usage(): Unit = {
    System.err.println("usage: Pipe2Lucene" +
      "\n\t<indexPath> - NGram's Lucene index path" +
      "\n\t<schemaFile> - NGram schema file" +
      "\n\t<schemaEncoding> - NGram schema file encoding" +
      "\n\t<pipeFile> - pipe file" +
      "\n\t[-encoding=<pipeEncoding>] - pipe file encoding. Default is utf-8" +
      "\n\t[--append] - append documents to an existing Lucene index"
    )
    System.exit(1)
  }

  if (args.length < 4) usage()

  val parameters = args.drop(4).foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
    }
  }

  val pipeEncoding = parameters.getOrElse("encoding", "utf-8")
  val append = parameters.contains("append")
  val p2l = new Pipe2Lucene()

  p2l.toLucene(args(0), args(1), args(2), args(3), pipeEncoding, append)
}

class Pipe2Lucene {
  def toLucene(indexPath: String,
               schemaFile: String,
               schemaEncoding: String,
               pipeFile: String,
               pipeEncoding: String,
               append: Boolean): Unit = {

    val index = new NGIndex(indexPath, indexPath, false)
    val writer = index.getIndexWriter(append)
    val schema = new NGSchema("schema", schemaFile, schemaEncoding)
    val codec = pipeEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val reader = Source.fromFile(pipeFile)(decoder)

    reader.getLines().zipWithIndex.foreach {
      case (line,idx) =>
        if (idx % 10000 == 0) println(s"+++$idx")
        try {
          NGrams.indexDocument(index, writer, schema, line)
        } catch {
          case ex:Exception =>
            Console.err.println(s"Skipping line [$line] => ${ex.getMessage()}")
        }
    }

    writer.flush()
    print("Optimizing index ...")
    writer.forceMerge(1) // optimize index
    println("OK")
    writer.close()
    reader.close()
  }
}

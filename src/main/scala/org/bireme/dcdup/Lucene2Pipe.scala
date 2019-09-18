/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.Writer
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import br.bireme.ngrams.NGSchema
import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, MultiBits}
import org.apache.lucene.store.FSDirectory

//import scala.jdk.CollectionConverters._ //scala 2.13.0
import scala.collection.JavaConverters._

/**
  * Generate a pipe file with the documents stored in a Lucene index
*/
object Lucene2Pipe extends App {
  private def usage(): Unit = {
    System.err.println("usage: Lucene2Pipe" +
      "\n\t-index=<indexPath> - NGram's Lucene index path" +
      "\n\t-schema=<schemaFile> - NGram schema file" +
      "\n\t-pipeFile=<pipeFile> - output pipe file" +
      "\n\t[-schemaFileEncod=<schemaEncoding>] - NGram schema file encoding" +
      "\n\t[-pipeFileEncod=<pipeFileEncod>] - output pipe file encoding. Default is utf-8)"
    )
    System.exit(1)
  }

  if (args.length < 3) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  val index = parameters("index")
  val schema = parameters("schema")
  val pipeFile = parameters("pipeFile")
  val schemaFileEncod = parameters.getOrElse("schemaFileEncod", "utf-8")
  val pipeFileEncod = parameters.getOrElse("pipeFileEncod", "utf-8")

  generatePipe(index, schema, schemaFileEncod, pipeFile, pipeFileEncod)

  /**
  * Write a pipe file with the documents stored in a Lucene index
    * @param indexPath Lucene index path
    * @param schemaFile Ngrams schema file path
    * @param schemaEncoding Ngrams schema file encoding
    * @param pipeFile output pipe file
    * @param pipeEncoding output pipe file encoding
    */
  def generatePipe(indexPath: String,
                   schemaFile: String,
                   schemaEncoding: String,
                   pipeFile: String,
                   pipeEncoding: String): Unit = {

    val schema = new NGSchema("schema", schemaFile, schemaEncoding)
    val fields = schema.getPosNames.asScala.toList
    val out = Files.newBufferedWriter(Paths.get(pipeFile),
                                      Charset.forName(pipeEncoding))
    val directory = FSDirectory.open(Paths.get(indexPath))
    val reader = DirectoryReader.open(directory)
    //val liveDocs = MultiFields.getLiveDocs(reader) Lucene version before 8.0.0
    val liveDocs = MultiBits.getLiveDocs(reader)  // Lucene version 8.0.0

    (0 until reader.maxDoc()).foreach {
    //(0 until 1).foreach {
      id =>
        if ((liveDocs == null) || liveDocs.get(id))
          exportDoc(reader.document(id), fields, out)
        if (id % 100000 == 0) println(s"+++$id")
    }

    reader.close()
    directory.close()
    out.close()
  }

  /**
  * Write a Lucene document into a pipe file
    * @param doc Lucene document
    * @param fields the name of the document fields
    * @param out pipe file writer
    */
  private def exportDoc(doc: Document,
                        fields: List[(Integer,String)],
                        out: Writer): Unit = {
    val pipe = fields.zipWithIndex.foldLeft[String]("") {
      case (str, ((pos,fldName), idx)) =>
        val str1 = (idx until pos).foldLeft[String](str) {
          case (str2, _) => str2 + "|"
        }
        val content = doc.get(fldName + "~notnormalized")
        str1 + (if (str1.isEmpty) "" else "|") +
                              (if (content == null) "" else content)
    }
    out.write("\n")
    out.write(pipe)
  }
}

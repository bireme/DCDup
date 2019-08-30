/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import br.bireme.ngrams.{Field, NGSchema}

import java.io.File

import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store.FSDirectory

import scala.jdk.CollectionConverters._  //scala 2.13.0
//import scala.collection.JavaConverters._

object TestIndex extends App {
  private def usage(): Unit = {
    Console.err.println("TestIndex tests if the Lucene index documents follow a DeDup schema specification")
    Console.err.println("usage: TestIndex")
    Console.err.println("\t-index=<indexDir> - Lucene index path")
    Console.err.println("\t-schema=<schemaFile> = file path with schema specification")
    Console.err.println("\t[-schemaFileEncod=<schemaEncoding>] - schema file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 2) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }

  val index = parameters("index")
  val schema = parameters("schema")
  val schemaFileEncod = parameters.getOrElse("schemaFileEncod", "utf-8")

  test(index, schema, schemaFileEncod)

  def test(indexDir: String,
           schemaFile: String,
           schemaEncoding: String): Unit = {
    val schema = new NGSchema("", schemaFile, schemaEncoding)
    val parameters = schema.getParameters
    val fields: Map[String, Field] = parameters.getNameFields.asScala.toMap

    val directory = FSDirectory.open(new File(indexDir).toPath)
    val ireader = DirectoryReader.open(directory)

    val bad = (0 until ireader.maxDoc) exists {
      id =>
        val doc = ireader.document(id)
        val bad = badDocument(doc, fields)

        if (id % 100000 == 0) println(s"+++ $id")
        if (bad) println(s"BAD DOCUMENT => id: ${doc.get("id")}")
        bad
    }
    ireader.close()

    if (!bad) println("Index is OK!")
  }

  private def badDocument(doc: Document,
                          fields: Map[String, Field]): Boolean = {
    fields.values.exists {
      field => !checkRequiredField(doc, field)
    }
  }

  private def checkRequiredField(doc: Document,
                                 field: Field): Boolean = {
    val reqFieldName = field.requiredField
    val recFieldContent = doc.get(reqFieldName)

    (reqFieldName == null) || reqFieldName.isEmpty ||
    ((recFieldContent != null) && (!recFieldContent.isEmpty))
  }
}

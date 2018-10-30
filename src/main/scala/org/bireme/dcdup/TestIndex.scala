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

import scala.collection.JavaConverters._

object TestIndex extends App {
  private def usage(): Unit = {
    Console.err.println("usage: TestIndex <indexDir> <schemaFile> [<schemaEncoding>]")
    System.exit(1)
  }

  if (args.length < 2) usage()

  val encoding = if (args.length > 2) args(2) else "utf-8"

  test(args(0), args(1), encoding)

  def test(indexDir: String,
           schemaFile: String,
           schemaEncoding: String): Unit = {
    val schema = new NGSchema("", schemaFile, schemaEncoding)
    val parameters = schema.getParameters()
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

    (reqFieldName == null) || (reqFieldName.isEmpty) ||
    ((recFieldContent != null) && (!recFieldContent.isEmpty))
  }
}

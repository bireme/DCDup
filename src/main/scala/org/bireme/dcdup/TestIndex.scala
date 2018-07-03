/*=========================================================================

    Copyright © 2017 BIREME/PAHO/WHO

    This file is part of DCDup.

    DCDup is free software: you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public License as
    published by the Free Software Foundation, either version 2.1 of
    the License, or (at your option) any later version.

    DCDup is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with DCDup. If not, see <http://www.gnu.org/licenses/>.

=========================================================================*/

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
      field => !checkContentPresence(doc, field) ||
               !checkRequiredField(doc, field)
    }
  }

  private def checkContentPresence(doc: Document,
                                   field: Field): Boolean = {
    ((doc.get(field.name) != null) &&
     (doc.get(field.name + "~notnormalized") != null)) ||
    (field.presence != Field.Status.REQUIRED)
  }

  private def checkRequiredField(doc: Document,
                                 field: Field): Boolean = {
    val reqFieldName = field.requiredField
    val recFieldContent = doc.get(reqFieldName)

    (reqFieldName == null) || (reqFieldName.isEmpty) ||
    ((recFieldContent != null) && (!recFieldContent.isEmpty))
  }
}

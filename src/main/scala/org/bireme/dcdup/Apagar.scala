package org.bireme.dcdup

import java.nio.file.Files

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{Document, Field, TextField}
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, Term}
import org.apache.lucene.store.FSDirectory

object Apagar extends App {
  val analyzer = new StandardAnalyzer()
  val indexPath = Files.createTempDirectory("tempIndex")
  val directory = FSDirectory.open(indexPath)
  val config = new IndexWriterConfig(analyzer)
  val iwriter = new IndexWriter(directory, config)
  val doc = new Document()
  doc.add(new TextField("id", "id1", Field.Store.YES))
  doc.add(new Field("fieldname", "This is the text 1", TextField.TYPE_STORED))
  iwriter.addDocument(doc)
  val doc2 = new Document()
  doc2.add(new TextField("id", "id1", Field.Store.YES))
  doc2.add(new Field("fieldname", "This is the text 2", TextField.TYPE_STORED))
  iwriter.updateDocument(new Term("id", "id1"), doc2)
  iwriter.close()
}

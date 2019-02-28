/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.{BufferedWriter, File}
import java.nio.charset.Charset
import java.nio.file.Files

import br.bireme.ngrams.{NGAnalyzer, NGrams}
import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader, MultiFields}
import org.apache.lucene.search.spell.NGramDistance
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Bits

import scala.collection.immutable.TreeMap

/**
* Look for documents whose field is similar to the input text
  *
  * Author: Heitor Barbieri
  * Date: 20190227
  */
object SimilarFieldDocs extends App {
  private def usage(): Unit = {
    System.err.println("usage: SimilarFieldDocs")
    System.err.println("\t\t-fldText=<text> - input text used to find similar documents")
    System.err.println("\t\t-index=<path> - path to the Lucene index")
    System.err.println("\t\t-fieldName=<name> - document field used to compare with the input text")
    System.err.println("\t\t-outFile=<file> - the output file where the similar documents will be placed")
    System.err.println("\t\t[-outSize=<max>] - the maximum number of similar documents that will be outputed")
    System.err.println("\t\t[--notNormalize] - do not normalize the input text")
    System.exit(1)
  }

  val parameters = args.foldLeft[Map[String, String]](Map()) {
    case (map, par) =>
      if (par.isEmpty) map
      else {
        val split = par.split(" *= *", 2)
        if (split.length == 1) {
          println(s"split(0)=${split(0)}")
          map + ((split(0).substring(2), ""))
        }
        else if (split.length == 2) map + ((split(0).substring(1), split(1)))
        else {
          usage()
          map
        }
      }
  }

  if (parameters.size < 4) usage()

  val fldText: String = parameters("fldText")
  val index: String = parameters("index")
  val fieldName: String = parameters("fieldName")
  val outFile: String = parameters("outFile")
  val outSize: Int = parameters.getOrElse("outSize", "10").toInt
  val notNormalize: Boolean = parameters.getOrElse("notNormalize", "false").toBoolean

  findSimilars(fldText, index, fieldName, outFile, notNormalize)

  /**
  * Find documents whose field is similiar to the input text
    * @param fldText input text used to find similar documents
    * @param index path to the Lucene index
    * @param fldName document field used to compare with the input text
    * @param outFile the output file where the similar documents will be placed
    * @param notNormalize true if it is to not normalize the input text, false to normalize the input text
    */
  def findSimilars(fldText: String,
                   index: String,
                   fldName: String,
                   outFile: String,
                   notNormalize: Boolean): Unit = {
    val inText: String = if (notNormalize) fldText.trim
      else br.bireme.ngrams.Tools.limitSize(
      br.bireme.ngrams.Tools.normalize(fldText, NGrams.OCC_SEPARATOR), NGrams.MAX_NG_TEXT_SIZE).trim
    val ngDistance = new NGramDistance(new NGAnalyzer().getNgramSize)
    val iterator: DocumentIterator = new DocumentIterator(index)
    val writer: BufferedWriter = Files.newBufferedWriter(
      new File(outFile).toPath, Charset.forName("utf-8"))
    val reader: DirectoryReader = DirectoryReader.open(FSDirectory.open(new File(index).toPath))

    writer.write(fldText + "\n\n")

    val sim : Map[Float, Set[Int]] =
      findSimilars(inText, iterator, fldName, ngDistance, new TreeMap[Float,Set[Int]], 0, 0)

    TreeMap[Float, Set[Int]]()(Ordering[Float].reverse) ++ sim foreach {
      case (sim: Float, set: Set[Int]) => set foreach {
        id: Int =>
          val doc = reader.document(id)
          val field: String = {
            val aux: String = doc.get(fldName + "~notnormalized")
            if (aux == null) "" else aux
          }
          val did: String = {
            val aux: String = doc.get("id")
            if (aux == null) "" else aux
          }
          if (!field.isEmpty && !did.isEmpty) writer.write(s"$sim|$did|$field\n")
      }
    }

    reader.close()
    iterator.close()
    writer.close()
  }

  /**
  * Find documents whose field is similiar to the input text
    * @param inText input text used to find similar documents
    * @param iterator (id, document) iterator
    * @param fldName name of the field used to compare similarity with the input text
    * @param ngDistance object used to calculate the distance/similarity between texts
    * @param results auxiliary TreeMap
    * @param stored number of already stored document ids in the TreeMap
    * @param current order number of the current document to look for similiar ones
    * @return a TreeMap of (similariy,doc Id) tuples found of the similar documents
    */
  private def findSimilars(inText: String,
                           iterator: DocumentIterator,
                           fldName: String,
                           ngDistance: NGramDistance,
                           results: TreeMap[Float,Set[Int]],
                           stored: Int,
                           current: Int): TreeMap[Float,Set[Int]] = {
    if (current % 100000 == 0) println(s"+++$current")

    if (iterator.hasNext) {
      val (id: Int, doc: Document) = iterator.next()
//println(s"id=$id")
      val field: String = {
        val aux: String = doc.get(fldName)
        if (aux == null) "" else aux
      }
      val distance: Float = ngDistance.getDistance(inText, field)
      val (results2, added) = results.lastOption match {
        case Some(kv) =>
          if (stored < outSize) (addElement(id, distance, results), true)
          else {
            if (distance > kv._1) {
              val auxSet: Set[Int] = kv._2.tail
              val resultLess : TreeMap[Float, Set[Int]] = if (auxSet.isEmpty) results - kv._1
                                                      else results + (kv._1 -> kv._2.tail)
              (addElement(id, distance, resultLess), false)
            } else (results, false)
          }
        case None => (addElement(id, distance, results), true)
      }
      findSimilars(inText, iterator, fldName, ngDistance, results2, if (added) stored + 1 else stored, current + 1)
    } else results
  }

  /**
  * Add an (similarity,doc id) into the TreeSet
    * @param id Lucene internal document identification
    * @param distance similarity of two documents
    * @param results auxiliary TreeMap
    * @return the aux TreeMap with the element (similarity, doc id) inserted
    */
  private def addElement(id: Int,
                         distance: Float,
                         results: TreeMap[Float,Set[Int]]): TreeMap[Float,Set[Int]] = {
    results.get(distance) match {
      case Some(ids) => results + (distance -> (ids + id))
      case None => results + (distance -> Set(id))
    }
  }
}

/**
* Class that iterates over the documents of a Lucene index
  * @param indexPath - Lucene index path
  */
class DocumentIterator(indexPath: String) extends Iterator[(Int, Document)] {
  val reader: DirectoryReader = DirectoryReader.open(FSDirectory.open(new File(indexPath).toPath))
  val liveDocs: Bits = MultiFields.getLiveDocs(reader)
  val max: Int = reader.maxDoc()
  var cur: Int = 0
  var elem: Option[(Int, Document)] = getNext

  /**
  * Internal hasNext of the Iterator interface
    * @return true if has a next object of false otherwise
    */
  private def hasNext0: Boolean = {
    if (cur < max) {
      if ((liveDocs == null) || liveDocs.get(cur)) true       // there is no deleted document OR document is active
      else {                                                    // document is deleted
        cur += 1
        hasNext0
      }
    } else {
      close()
      false
    }
  }

  /**
  *
    * @return the next document from the index
    */
  private def getNext: Option[(Int,Document)] = {
    if (hasNext0) {
      cur += 1
      Some((cur - 1, reader.document(cur - 1)))
    } else None
  }

  /**
  * See Iterator documentation
    * @return true if has a next object of false otherwise
    */
  override def hasNext: Boolean = elem.isDefined

  /**
  * See Iterator documentation
    * @return return the next tuple (internal lucene doc id, document object)
    */
  override def next(): (Int,Document) = {
    if (hasNext) {
      val ret: (Int, Document) = elem.get
      elem = getNext
      ret
    }
    else throw new NoSuchElementException()
  }

  /**
  * Close open resource
    */
  def close(): Unit = {
    reader.close()
  }
}

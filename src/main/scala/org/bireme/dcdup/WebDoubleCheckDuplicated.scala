/*=========================================================================

    Copyright © 2016 BIREME/PAHO/WHO

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

import br.bireme.ngrams.{NGrams,NGIndex,NGSchema}

import java.io.{File,IOException}
import java.nio.file.{Files,Paths,StandardOpenOption}
import java.nio.charset.Charset
import java.util.Calendar

import org.apache.http.{HttpEntity,HttpResponse}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

import scala.io.Source

/** Checks a DeDup input piped file against itself to look for duplicated
  * documents and then against a Dedup index via DeDup webservice
  * to also look for duplicated docs.
  *
  * author: Heitor Barbieri
  * date: 20161208
*/
object WebDoubleCheckDuplicated extends App {
  private def usage(): Unit = {
    System.err.println("usage: WebDoubleCheckDuplicated " +
      "\n\t<pipeFile> - DeDup piped input file" +
      "\n\t<pipeFileEncoding> - pipe file character encoding" +
      "\n\t<schemaFile> - local schema file name" +
      "\n\t<schemaFileEncoding> - DeDup schema file character encoding" +
      "\n\t<DeDupBaseUrl> - DeDup url service" +
      "\n\t<indexName> - DeDup index name" +
      "\n\t<schemaName> - DeDup schema name" +
      "\n\t<outDupFile1> - duplicated records found in pipe file" +
      "\n\t<outDupFile2> - duplicated records found between pipe file and Dedup index" +
      "\n\t<outNoDupFile> - no duplicated records between (pipe file and itself) " +
      "and (pipe file and Dedup index)" +
      "\n\t[<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 10) usage()
  val outEncoding = if (args.length > 10) args(10) else "utf-8"

  doubleCheck(args(0), args(1), args(2), args(3), args(4), args(5),
              args(6), args(7), args(8), args(9), outEncoding)

  def doubleCheck(pipeFile: String,
                  pipeFileEncoding: String,
                  schemaFile: String,       // local file
                  schemaFileEncoding: String,
                  deDupBaseUrl: String,     // http://ts10vm.bireme.br:8180/DeDup/services/ or http://dedup.bireme.org/services
                  indexName: String,
                  schemaName: String,       // used in DeDup service
                  outDupFile1: String,
                  outDupFile2: String,
                  outNoDupFile: String,
                  outDupFileEncoding: String): Unit = {
    val time = Calendar.getInstance().getTimeInMillis().toString
    val ngSchema = new NGSchema(schemaFile, schemaFile, schemaFileEncoding)
    val tmpIndexPath = createTmpIndex(pipeFile, pipeFileEncoding, ngSchema, time)
    val tmpIndex = new NGIndex(tmpIndexPath, tmpIndexPath, true)

    // Self check
    check(tmpIndex, ngSchema, pipeFile, pipeFileEncoding, outDupFile1,
                                                             outDupFileEncoding)
    // Check using DeDup service
    remoteCheck(deDupBaseUrl, indexName, schemaName, pipeFile, pipeFileEncoding,
                                                outDupFile2, outDupFileEncoding)
    // Take duplicate no duplicated documents
    takeNoDuplicated(pipeFile, pipeFileEncoding, outDupFile1, outDupFile2,
                     outNoDupFile, outDupFileEncoding)
    // Delete temporary files
    deleteFile(new File(tmpIndexPath))
  }

  /** Given an input piped file and a NGrams index, looks for documents that
    * are in the input file that are similar to the ones stored in the index.
    *
    * @param ngIndex NGrams index path
    * @param ngSchema NGrams index/search configuration file
    * @param pipeFile input piped file used to look for similar docs
    * @param pipeFileEncoding input piped file character encoding
    * @param outDupFile output piped file with the similar documents
    * @param outDupFileEncoding output piped file character encoding
    */
  private def check(ngIndex: NGIndex,
                    ngSchema: NGSchema,
                    pipeFile: String,
                    pipeFileEncoding: String,
                    outDupFile: String,
                    outDupFileEncoding: String): Unit = {
    NGrams.search(ngIndex, ngSchema, pipeFile, pipeFileEncoding,
                  outDupFile, outDupFileEncoding)
  }

  /** Given an input piped file and a NGrams index, looks for documents that
    * are in the input file that are similar to the ones stored in the index
    * through DeDup web service.
    *
    * @param deDupBaseUrl url to DeDup webservice, usually http://dedup.bireme.org/services
    * @param indexName DeDup index name used to look for duplicates. See http://dedup.bireme.org/services/indexes
    * @param schemaName DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param pipeFile input piped file used to look for similar docs
    * @param pipeFileEncoding input piped file character encoding
    * @param outDupFile output piped file with the similar documents
    * @param outDupFileEncoding output piped file character encoding
    */
  private def remoteCheck(deDupBaseUrl: String,
                          indexName: String,
                          schemaName: String,
                          pipeFile: String,
                          pipeFileEncoding: String,
                          outDupFile: String,
                          outDupFileEncoding: String): Unit = {
    val quantity = 250 // Number of documents sent to each call of DeDup service
    val src = Source.fromFile(pipeFile, pipeFileEncoding)
    val dest = Files.newBufferedWriter(Paths.get(outDupFile),
                                       Charset.forName(outDupFileEncoding))

    new LineBatchIterator(src.getLines(), quantity).foreach {
      batch =>
        rcheck(deDupBaseUrl, indexName, schemaName, batch) match {
          case Some(response) => response.split("\\n").zipWithIndex.foreach {
            case (line,idx) =>
              if (idx == 0) dest.write("\n")
              dest.write(line)
          }
          case None => new IOException(batch)
        }
    }
    src.close()
    dest.close()
  }

  /** Checks some documents via DeDup webservice to look for similar docs.
    *
    * @param indexName DeDup index name used to look for duplicates. See http://dedup.bireme.org/services/indexes
    * @param schemaName DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param lines string having documents separated by new line character
    * @return an option with a json string having the duplicated documents
    */
  private def rcheck(baseUrl: String,
                     indexName: String,
                     schemaName: String,
                     lines: String): Option[String]= {
    val baseUrlTrim = baseUrl.trim
    val burl = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
    val httpClient = HttpClientBuilder.create().build()
    val post = new HttpPost(burl + "indexName/schemaName")
    val postingString = new StringEntity(lines)

    post.setEntity(postingString)
    post.setHeader("Content-type", "text/plain;charset=utf-8")

    val response = httpClient.execute(post)

    if (response.getStatusLine.getStatusCode == 200)
      Some(EntityUtils.toString(response.getEntity()))
    else None
  }

  /** Creates a temporary DeDup index.
    *
    * @param pipeFile piped file with documents that will populate the index
    * @param pipeFileEncoding piped file character encoding
    * @param ngSchema DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param time time string used as a suffix of the index name
    * @return the index name
    */
  private def createTmpIndex(pipeFile: String,
                             pipeFileEncoding: String,
                             ngSchema: NGSchema,
                             time: String): String = {
    val indexPath = s"/tmp/DCDup_$time"
    val ngIndex = new NGIndex(indexPath, indexPath, false)

    NGrams.index(ngIndex, ngSchema, pipeFile, pipeFileEncoding)
    indexPath
  }

  /** Creates a pipe file that is equal to the first one less the lines whose
   * identifiers are also in the second and third ones.
   *
   * @param pipeFile the first piped file (the original input)
   * @param pipeFileEncoding the first piped file character encoding
   * @param outDupFile1 the second piped file (checked with itself)
   * @param outDupFile2 the third piped file  (checked with DeDup index)
   * @param outNoDupFile the ouput piped file with no duplicated records
   * @param outDupFileEncoding the no duplicated record file character encoding
   */
  private def takeNoDuplicated(pipeFile: String,
                               pipeFileEncoding: String,
                               outDupFile1: String,
                               outDupFile2: String,
                               outNoDupFile: String,
                               outDupFileEncoding: String): Unit = {
    val auxIds = getIds(outDupFile1, outDupFileEncoding)
    val ids = auxIds ++ getIds(outDupFile2, outDupFileEncoding, onlyFirstId= true)
    val in = Source.fromFile(pipeFile, pipeFileEncoding)
    val out = Files.newBufferedWriter(Paths.get(outNoDupFile),
                                      Charset.forName(pipeFileEncoding))
    var first = true

    in.getLines().foreach {
      line => getIdFromLine(line) match {
        case Some(id) =>
          if (!ids.contains(id)) {
            if (first) first = false else out.write("\n")
            out.write(line)
          }
        case None => ()
      }
    }
    in.close()
    out.close()
  }

  /** Given an output piped file resulting from NGrams check and returns all
    * ids that identifies duplicated documents (if onlyFirstId is false) or
    * the lower one from the duple (if onlyFirstId is true)
    * @param outDupFile duplicated doc piped file
    * @param outDupFileEncoding duplicated doc piped file encoding
    * @param onlyFirstId if true returns only the first id from both of duplicated
    *                   docs otherwise it retuns both ids
    * @return a set of duplicated doc ids
    */
  private def getIds(outDupFile: String,
                     outDupFileEncoding: String,
                     onlyFirstId: Boolean = false): Set[String] = {
    val reader = Source.fromFile(outDupFile, outDupFileEncoding)
    val in = reader.getLines()

    // Skip two first header licenses
    in.drop(2)

    val outSet = in.foldLeft[Set[String]](Set()) {
      case (set, line) => getSimIdsFromLine(line) match {
        case Some((id1,id2)) =>
          if (onlyFirstId) set + id1
          else set + (id1, id2)
        case None => set
      }
    }
    reader.close()
    outSet
  }

  /** Given a NGrams output line (piped), retrives the two similar doc ids.
    *
    * @param line ids piped line
    * @return an option 
    */
  private def getSimIdsFromLine(line: String): Option[(String,String)] = {
    val linet = line.trim()

    if (linet.isEmpty) None
    else {
      val split = linet.split("\\|", 5)
      if (split.length != 5) throw new IOException(linet)
      Some((split(2), split(3)))
    }
  }

  /** Given a NGrams input line (piped), retrives the doc id.
    *
    * @param line ids piped line
    */
  private def getIdFromLine(line: String): Option[String] = {
    val linet = line.trim()

    if (linet.isEmpty) None
    else {
      val split = linet.split("\\|", 3)
      if (split.length != 3) throw new IOException(linet)
      Some(split(1))
    }
  }

  /** Deletes a standard file or a directory recursivelly
    *
    * @param file file or directory to be deleted
    */
  private def deleteFile(file: File): Unit = {
    val contents = file.listFiles()
    if (contents != null) contents.foreach(deleteFile(_))
    file.delete()
  }
}

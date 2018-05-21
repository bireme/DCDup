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
import java.nio.charset.{Charset, CodingErrorAction}
import java.nio.file.{Files,Paths}
import java.util.Calendar

import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.{HttpGet,HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

import scala.io.{Codec, Source}
import scala.collection.JavaConverters._

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
      "\n\t<DeDupBaseUrl> - DeDup url service" +
      "\n\t<indexName> - DeDup index name" +
      "\n\t<schemaName> - DeDup schema name" +
      "\n\t<outDupFile1> - duplicated records found in pipe file" +
      "\n\t<outDupFile2> - duplicated records found between pipe file and Dedup index" +
      "\n\t<outNoDupFile> - no duplicated records between (pipe file and itself) " +
      "and (pipe file and DeDup index)" +
      "\n\t[<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 8) usage()
  val outEncoding = if (args.length > 8) args(8) else "utf-8"

  val dup = new WebDoubleCheckDuplicated()
  dup.doubleCheck(args(0), args(1), args(2), args(3), args(4),
                  args(5), args(6), args(7), outEncoding)
}

/** Class to check a DeDup input piped file against itself to look for duplicated
  * documents and then against a Dedup index via DeDup webservice
  * to also look for duplicated docs.
  *
  * author: Heitor Barbieri
  * date: 20161208
*/
class WebDoubleCheckDuplicated {
  def doubleCheck(pipeFile: String,
                  pipeFileEncoding: String,
                  deDupBaseUrl: String,     // http://ts10vm.bireme.br:8180/DeDup/services/ or http://dedup.bireme.org/services
                  indexName: String,
                  schemaName: String,       // used in DeDup service
                  outDupFile1: String,
                  outDupFile2: String,
                  outNoDupFile: String,
                  outDupFileEncoding: String): Unit = {
    // Verifying pipe file integrity
    println("\nVerifying pipe file integrity")
    val goodFileName = File.createTempFile("good", "").getPath()
    val badFileName = File.createTempFile("bad", "").getPath()
    val (good,bad) = VerifyPipeFile.check(pipeFile, pipeFileEncoding,
      deDupBaseUrl + "/schema/" + schemaName, goodFileName, badFileName)

    println(s"Checking duplicates for $good documents")
    if (bad > 0) println(s"Skipping $bad documents. See file: $badFileName")

    val schemaStr = loadSchema(deDupBaseUrl, schemaName)
//println(s"[$schemaStr]")
    val ngSchema = new NGSchema(schemaName, schemaStr)

    // Self check
    println("\nSelf check")
    localCheck(ngSchema, pipeFile, pipeFileEncoding, outDupFile1,
                                                             outDupFileEncoding)
    // Check using DeDup service
    println("\nRemote check")
    remoteCheck(deDupBaseUrl, indexName, schemaName, pipeFile, pipeFileEncoding,
                                                outDupFile2, outDupFileEncoding)
    // Take duplicate no duplicated documents
    takeNoDuplicated(pipeFile, pipeFileEncoding, outDupFile1, outDupFile2,
                     outNoDupFile, outDupFileEncoding)
  }

  /** Given an input piped file and a NGrams index, looks for documents that
    * are in the input file that are similar to the ones stored in the index.
    *
    * @param schemaFile NGrams index/search configuration file
    * @param schemaFileEncoding Schema file encoding
    * @param pipeFile input piped file used to look for similar docs
    * @param pipeFileEncoding input piped file character encoding
    * @param outDupFile output piped file with the similar documents
    * @param outDupFileEncoding output piped file character encoding
    */
  def localCheck(schemaFile: String,
                 schemaFileEncoding: String,
                 pipeFile: String,
                 pipeFileEncoding: String,
                 outDupFile: String,
                 outDupFileEncoding: String): Unit = {
    val codec = schemaFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val src = Source.fromFile(schemaFile)(decoder)
    val schemaStr = src.getLines().mkString("\n")
    src.close()

    val ngSchema = new NGSchema("tmpSchema", schemaStr)
    localCheck(ngSchema, pipeFile, pipeFileEncoding, outDupFile, outDupFileEncoding)
  }

  /** Given an input piped file and a NGrams index, looks for documents that
    * are in the input file that are similar to the ones stored in the index.
    *
    * @param ngSchema NGrams index/search configuration file
    * @param pipeFile input piped file used to look for similar docs
    * @param pipeFileEncoding input piped file character encoding
    * @param outDupFile output piped file with the similar documents
    * @param outDupFileEncoding output piped file character encoding
    */
  def localCheck(ngSchema: NGSchema,
                 pipeFile: String,
                 pipeFileEncoding: String,
                 outDupFile: String,
                 outDupFileEncoding: String): Unit = {
    val time = Calendar.getInstance().getTimeInMillis().toString
    val tmpIndexPath = "/tmp/" + "DCDup_" + time
    val tmpIndex = new NGIndex("tmpIndex", tmpIndexPath, false)
    val indexWriter = tmpIndex.getIndexWriter(false)
    val codec = pipeFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val src = Source.fromFile(pipeFile)(decoder)
    val dest = Files.newBufferedWriter(Paths.get(outDupFile),
                                       Charset.forName(outDupFileEncoding))
    var cur = 0
    indexWriter.commit()

    src.getLines().foreach(line =>
      if (!line.trim().isEmpty) {
        //println("line=" + line + "\n")
        NGrams.search(tmpIndex, ngSchema, line, false).asScala.foreach {
          case line2 =>
            if (cur > 0) dest.write("\n")
            dest.write(line2)
        }
        NGrams.indexDocument(tmpIndex, indexWriter, ngSchema, line)
        indexWriter.commit()
        if (cur % 1000 == 0) println(s"<<< $cur")
        cur += 1
      }
    )
    indexWriter.close()
    src.close()
    dest.close()

    // Delete temporary files
    deleteFile(new File(tmpIndexPath))
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
  def remoteCheck(deDupBaseUrl: String,
                  indexName: String,
                  schemaName: String,
                  pipeFile: String,
                  pipeFileEncoding: String,
                  outDupFile: String,
                  outDupFileEncoding: String): Unit = {
    val quantity = 250 // Number of documents sent to each call of DeDup service
    val codec = pipeFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val src = Source.fromFile(pipeFile)(decoder)
    val dest = Files.newBufferedWriter(Paths.get(outDupFile),
                                       Charset.forName(outDupFileEncoding))
    val isUtf8 = outDupFileEncoding.trim.toLowerCase.equals("utf-8")
    val undefined = 150.toChar
    var cur = 0

    new LineBatchIterator(src.getLines(), quantity).foreach {
      batch =>
        println(s"<<< $cur")
        val remote = rcheck(deDupBaseUrl, indexName, schemaName, batch)
        if (!remote.isEmpty) {
          if (cur != 0) dest.write("\n")
          if (isUtf8) dest.write(remote)
          else {
            val cleanedRemote =  remote.map {
              ch =>
                val chi = ch.toInt
                if (chi < 256) ch else undefined
            }
            dest.write(cleanedRemote)
          }
        }
        cur += quantity
    }
    src.close()
    dest.close()
  }


  /** Checks some documents via DeDup webservice to look for similar docs.
    *
    * @param baseUrl DeDup service url. For example: http://dedup.bireme.org/services or http://ts10vm.bireme.br:8180/services
    * @param indexName DeDup index name used to look for duplicates. See http://dedup.bireme.org/services/indexes
    * @param schemaName DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param lines string having documents separated by new line character
    * @return a json string having the duplicated documents
    */
  private def rcheck(baseUrl: String,
                     indexName: String,
                     schemaName: String,
                     lines: String): String = {
//println(s"lines=[$lines]")
    val baseUrlTrim = baseUrl.trim
    val burl = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
    val httpClient = HttpClientBuilder.create().build()
    val post = new HttpPost(burl + "raw/duplicates/" + indexName + "/" + schemaName)
    val postingString = new StringEntity(lines, "UTF-8")

    post.setEntity(postingString)
    post.setHeader("Content-type", "text/plain;charset=utf-8")
    val response = httpClient.execute(post)
    val statusCode = response.getStatusLine.getStatusCode
    val ret = if (statusCode == 200) {
      val content = EntityUtils.toString(response.getEntity(), "utf-8").trim()
      if (content.startsWith("ERROR:")) throw new IOException("lines=" + lines +
        "\nexplanation=" + content)
      content
    } else throw new IOException(s"http post response code:$statusCode")

    httpClient.close()
//println(s"ret=[$ret]")
    ret
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
    val ids = getIds(outDupFile1, outDupFileEncoding, allowSameId = false) ++
              getIds(outDupFile2, outDupFileEncoding, onlyFirstId = true)
    val codec = pipeFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val in = Source.fromFile(pipeFile)(decoder)
    val out = Files.newBufferedWriter(Paths.get(outNoDupFile),
                                      Charset.forName(pipeFileEncoding))
    var first = true

    in.getLines().foreach(
      line => getIdFromLine(line).foreach(id =>
        if (!ids.contains(id)) {
          if (first) first = false else out.write("\n")
          out.write(line)
        } else ()
      )
    )
    in.close()
    out.close()
  }

  /** Given an output piped file resulting from NGrams check and returns all
    * ids that identifies duplicated documents (if onlyFirstId is false) or
    * the lower one from the duple (if onlyFirstId is true)
    * @param outDupFile duplicated doc piped file
    * @param outDupFileEncoding duplicated doc piped file encoding
    * @param dropLines number of drop lines to remove the header
    * @param onlyFirstId if true returns only the first id from both of duplicated
    *                   docs otherwise it retuns both ids
    * @param allowSameId if true both ids can be the same, if false only different
    *                    ids are included
    * @return a set of duplicated doc ids
    */
  private def getIds(outDupFile: String,
                     outDupFileEncoding: String,
                     dropLines: Int = 0,
                     onlyFirstId: Boolean = false,
                     allowSameId: Boolean = false): Set[String] = {
    val codec = outDupFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val reader = Source.fromFile(outDupFile)(decoder)
    val in = reader.getLines()

    // Skip header licenses lines
    in.drop(dropLines)

    val outSet = in.foldLeft[Set[String]](Set()) {
      case (set, line) => getSimIdsFromLine(line) match {
        case Some((id1,id2)) =>
          if (onlyFirstId) set + id1
          else if (allowSameId) (set + (id1, id2))
          else if (id1.equals(id2)) set else (set + (id1, id2))
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

  /**
    * Loads a schema file from the DeDup server
    *
    * @param deDupBaseUrl url to DeDup webservice, usually http://dedup.bireme.org/services
    * @param indexName DeDup index name used to look for duplicates. See http://dedup.bireme.org/services/indexes
    * @param schemaName DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @return the schema as a String
    */
  private def loadSchema(baseUrl: String,
                         schemaName: String): String = {
    val baseUrlTrim = baseUrl.trim
    val burl = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
    val schemaUrl = burl + "schema/xml/" +  schemaName
    val httpClient = HttpClientBuilder.create().build()
    val get = new HttpGet(schemaUrl)

    get.setHeader("Content-type", "text/plain;charset=utf-8")
    val response = httpClient.execute(get)
    val statusCode = response.getStatusLine.getStatusCode
    val ret = if (statusCode == 200) {
      val content = EntityUtils.toString(response.getEntity())
      if (content.startsWith("ERROR:")) throw new IOException(content)
      content
    } else throw new IOException(s"url=$schemaUrl statusCode=$statusCode")

    httpClient.close()
    ret
  }
}

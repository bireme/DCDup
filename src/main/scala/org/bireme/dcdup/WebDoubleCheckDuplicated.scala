/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io._
import java.nio.charset.{Charset, CodingErrorAction}

import br.bireme.ngrams.NGSchema
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
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
      "\n\t-pipeFile=<pipeFile> - DeDup piped input file" +
      "\n\t-dedupUrl=<DeDupBaseUrl> - DeDup url service. For ex: http://dedup.bireme.org/services" +
      "\n\t-index=<indexName> - DeDup index name" +
      "\n\t-schema=<schemaName> - DeDup schema name" +
      "\n\t-outDupFile1=<outDupFile1> - duplicated records found in pipe file" +
      "\n\t-outDupFile2=<outDupFile2> - duplicated records found between pipe file and Dedup index" +
      "\n\t-outNoDupFile=<outNoDupFile> - no duplicated records between (pipe file and itself) " +
      "and (pipe file and DeDup index)" +
      "\n\t[-pipeFileEncod=<pipeFileEncoding>] - pipe file character encoding. Default is utf-8" +
      "\n\t[-outDupEncod=<outDupEncoding>] - output file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 7) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }
  val pipeFile = parameters("pipeFile")
  val dedupUrl = parameters("dedupUrl")
  val index = parameters("index")
  val schema = parameters("schema")
  val outDupFile1 = parameters("outDupFile1")
  val outDupFile2 = parameters("outDupFile2")
  val outNoDupFile = parameters("outNoDupFile")
  val pipeFileEncod = parameters.getOrElse("pipeFileEncod", "utf-8")
  val outDupEncod = parameters.getOrElse("outDupEncod", "utf-8")

  val dup = new WebDoubleCheckDuplicated()
  dup.doubleCheck(pipeFile, pipeFileEncod, dedupUrl, index, schema,
                  outDupFile1, outDupFile2, outNoDupFile, outDupEncod)
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
                  indexName: String,        // Verifying pipe file integrity
                  schemaName: String,       // used in DeDup service
                  outDupFile1: String,
                  outDupFile2: String,
                  outNoDupFile: String,
                  outDupFileEncoding: String): Unit = {
    val schemaStr = loadSchema(deDupBaseUrl, schemaName)
//println(s"[$schemaStr]")
    val ngSchema = new NGSchema(schemaName, schemaStr)

    // Self check
    println("\nSelf check")
    CheckDuplicated.checkDuplicated(pipeFile, pipeFileEncoding , None, ngSchema, outDupFile1,
      outNoDupFile + "_self", outDupFileEncoding)

    // Check using DeDup service
    println("\nRemote check")
    remoteCheck(deDupBaseUrl, indexName, schemaName, pipeFile, pipeFileEncoding,
                                                outDupFile2 + "_tmp", outDupFileEncoding)

    println("OK\nPost processing remote duplicated files ... ")
    val dupIds: Map[String, Set[String]] =
      CheckDuplicated.postProcessDup(outDupFile2 + "_tmp", outDupFile2, outDupFileEncoding)

    println("OK\nPost processing no duplicated remote files ... ")
    val idsDup: Set[String] = dupIds.foldLeft(Set[String]()) ((set, kv) => set ++ (kv._2 + kv._1))
    CheckDuplicated.postProcessNoDup(pipeFile, pipeFileEncoding, ngSchema,
                                     outNoDupFile + "_remote", outDupFileEncoding, idsDup)

    // Remove duplication in the no duplicated documents file
    CheckDuplicated.takeNoDuplicated(ngSchema, outNoDupFile + "_self", outNoDupFile + "_remote",
      outNoDupFile, outDupFileEncoding)

    // Delete pre-processed output files
    CheckDuplicated.deleteFile(new File(outDupFile2 + "_tmp"))
    CheckDuplicated.deleteFile(new File(outNoDupFile + "_self"))
    CheckDuplicated.deleteFile(new File(outNoDupFile + "_remote"))
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
    val codAction = CodingErrorAction.REPLACE
    val encoder = Charset.forName(outDupFileEncoding).newEncoder()
                  .onMalformedInput(codAction)
                  .onUnmappableCharacter(codAction)
    val decoder = Charset.forName(pipeFileEncoding).newDecoder()
                  .onMalformedInput(codAction)
                  .onUnmappableCharacter(codAction)
    val src = Source.fromFile(pipeFile)(decoder)
    val dest = new BufferedWriter(new OutputStreamWriter(
                 new FileOutputStream(outDupFile), encoder))
    var cur = 0

    new LineBatchIterator(src.getLines(), quantity).foreach {
      batch =>
        println(s"<<< $cur")
        val remote = rcheck(deDupBaseUrl, indexName, schemaName, batch)
        if (!remote.isEmpty) {
          if (cur != 0) dest.write("\n")
          dest.write(remote)
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
    * @return a piped string having the duplicated documents
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
      val content = EntityUtils.toString(response.getEntity, "utf-8").trim()
      if (content.startsWith("ERROR:")) {
//println(s"content=[$content] lines=[$lines]")
        val split = lines.split(" *\n *").map(_.trim).filter(!_.isEmpty)
        split.length match {
          case 0 => ""
          case 1 =>
            System.err.println("Skipping line: " + split.head)
            ""
          case _ =>
            split.foldLeft("") {
              case (str, line) => str + rcheck(baseUrl, indexName, schemaName, line)
            }
        }
      } else content
    } else throw new IOException(s"http post response code:$statusCode")

    httpClient.close()
//println(s"ret=[$ret]")
    ret
  }

  /**
    * Loads a schema file from the DeDup server
    *
    * @param baseUrl url to DeDup webservice, usually http://dedup.bireme.org/services
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
      val content = EntityUtils.toString(response.getEntity)
      if (content.startsWith("ERROR:")) throw new IOException(content)
      content
    } else throw new IOException(s"url=$schemaUrl statusCode=$statusCode")

    httpClient.close()
    ret
  }
}

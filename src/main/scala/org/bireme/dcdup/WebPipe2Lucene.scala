/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.{File, IOException}
import java.nio.charset.{Charset, CodingErrorAction}

import scala.io.Source

import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

/**
  * Create a remote DeDup index from a piped file
  *
  * author: Heitor Barbieri
  */
object WebPipe2Lucene extends App {
  private def usage(): Unit = {
    System.err.println("usage: WebPipe2Lucene " +
      "\n\t-pipe=<pipeFile> - DeDup piped input file" +
      "\n\t-pipeFileEncod=<pipeFileEncoding> - pipe file character encoding" +
      "\n\t-dedupUrl=<DeDupBaseUrl> - DeDup url service" +
      "\n\t-index=<indexName> - DeDup index Name" +
      "\n\t-schema=<schemaName> - DeDup schema name" +
      "\n\t[--resetIndex]")
    System.exit(1)
  }

  if (args.length < 5) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }
  val pipe = parameters("pipe")
  val pipeFileEncod = parameters("pipeFileEncod")
  val dedupUrl = parameters("dedupUrl")
  val index = parameters("index")
  val schema = parameters("schema")
  val resetIndex = parameters.contains("resetIndex")

  convert(pipe, pipeFileEncod, dedupUrl, index, schema, resetIndex)

  def convert(pipeFile: String,
              pipeFileEncoding: String,
              deDupBaseUrl: String,     // http://ts10vm.bireme.br:8180/DeDup/services/ or http://dedup.bireme.org/services
              indexName: String,
              schemaName: String,
              resIndex: Boolean): Unit = {
    // Verifying pipe file integrity
    println("\nVerifying pipe file integrity")
    val goodFileName = File.createTempFile("good", "").getPath
    val badFileName = File.createTempFile("bad", "").getPath
    val (good,bad) = VerifyPipeFile.checkRemote(pipeFile, pipeFileEncoding,
      deDupBaseUrl + "/schema/" + schemaName, goodFileName, badFileName)
    println(s"Using $good documents")
    if (bad > 0) println(s"Skipping $bad documents. See file: $badFileName\n")

    if (resIndex) resetIndex(deDupBaseUrl, indexName)

    val quantity = 1000 // Number of documents sent to each call of DeDup service
    val codAction = CodingErrorAction.REPLACE
    val decoder = Charset.forName(pipeFileEncoding).newDecoder()
                  .onMalformedInput(codAction)
                  .onUnmappableCharacter(codAction)
    val src = Source.fromFile(pipeFile)(decoder)
    var cur = 0

    new LineBatchIterator(src.getLines(), quantity).foreach {
        batch =>
          println(s"<<< $cur")
          rsend(deDupBaseUrl, indexName, schemaName, batch)
          cur += quantity
    }
    src.close()
  }

  /** Re-initialize the index, turning it size equals to zero.
    *
    * @param baseUrl DeDup service url. For example: http://dedup.bireme.org/services or http://ts10vm.bireme.br:8180/services
    * @param indexName DeDup index name
    */
  def resetIndex(baseUrl: String,
                 indexName: String): Unit = {
    val baseUrlTrim = baseUrl.trim
    val burl = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
    val resetUrl = burl + "reset/" +  indexName
    val httpClient = HttpClientBuilder.create().build()
    val get = new HttpGet(resetUrl)
    get.setHeader("Content-type", "text/plain;charset=utf-8")
    val response = httpClient.execute(get)
    val statusCode = response.getStatusLine.getStatusCode

    if (statusCode == 200) {
      val content = EntityUtils.toString(response.getEntity)
      if (content.startsWith("ERROR:")) throw new IOException(content)
    } else throw new IOException(s"status code:$statusCode")

    httpClient.close()
  }

  /** Add some documents into the index via DeDup webservice.
    *
    * @param baseUrl DeDup service url. For example: http://dedup.bireme.org/services or http://ts10vm.bireme.br:8180/services
    * @param indexName DeDup index name
    * @param schemaName DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param lines string having piped documents separated by new line character
    * @return a json string having the insertion status
    */
  private def rsend(baseUrl: String,
                    indexName: String,
                    schemaName: String,
                    lines: String): String = {
    val baseUrlTrim = baseUrl.trim
    val burl = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
    val httpClient = HttpClientBuilder.create().build()
    val post = new HttpPost(burl + "putDocs/" + "/" + indexName + "/" + schemaName)
    val postingString = new StringEntity(lines, "utf-8")
//println(s"linhas=$lines")
    post.setEntity(postingString)
    post.setHeader("Content-type", "text/plain; charset=utf-8")
    val response = httpClient.execute(post)
    val statusCode = response.getStatusLine.getStatusCode
    val ret = if (statusCode == 200) {
      val content = EntityUtils.toString(response.getEntity)
      if (content.startsWith("ERROR:")) throw new IOException(content)
      val respo = optimizeIndex(burl, indexName)
      if (respo.startsWith("ERROR:")) throw new IOException(respo)
      content
    } else throw new IOException(s"status code:$statusCode")

    httpClient.close()
    ret
  }

  /** Optimize the new index
    *
    * @param baseUrl DeDup service url. For example: http://dedup.bireme.org/services or http://ts10vm.bireme.br:8180/services
    * @param indexName DeDup index name
    * @return "OK" if ok or "ERROR: <mess>" if not
    */
  private def optimizeIndex(baseUrl: String,
                            indexName: String): String = {
    val baseUrlTrim = baseUrl.trim
    val burl = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
    val httpClient = HttpClientBuilder.create().build()
    val get = new HttpGet(burl + "optimize/" + "/" + indexName)
    get.setHeader("Content-type", "text/plain; charset=utf-8")
    val response = httpClient.execute(get)
    val statusCode = response.getStatusLine.getStatusCode
    val ret = if (statusCode == 200) {
      val content = EntityUtils.toString(response.getEntity)
      if (content.startsWith("ERROR:")) throw new IOException(content)
      content
    } else throw new IOException(s"status code:$statusCode")

    httpClient.close()
    ret
  }
}

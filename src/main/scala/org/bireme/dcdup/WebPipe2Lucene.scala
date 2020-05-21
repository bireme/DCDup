/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.{File, IOException}
import scala.io.Source
import scalaj.http.{Http, HttpOptions, HttpResponse}

/**
  * Create a remote DeDup index from a piped file
  *
  * author: Heitor Barbieri
  */
object WebPipe2Lucene extends App {
  private def usage(): Unit = {
    System.err.println("usage: WebPipe2Lucene " +
      "\n\t-dedupUrl=<DeDupBaseUrl> - DeDup url service  (https://dedup.bireme.org/services')" +
      "\n\t-index=<indexName> - DeDup index name" +
      "\n\t-schema=<schemaName> - DeDup schema name" +
      "\n\t-pipe=<pipeFile> - DeDup piped input file" +
      "\n\t[-pipeEncoding=<pipeFileEncoding>] - pipe file character encoding" +
      "\n\t[--append]")
    System.exit(1)
  }

  if (args.length < 4) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }
  val dedupUrl = parameters("dedupUrl")
  val index = parameters("index")
  val schema = parameters("schema")
  val pipe = parameters("pipe")
  val pipeEncoding = parameters.getOrElse("pipeEncoding", "utf-8")
  val append = parameters.contains("append")

  convert(pipe, pipeEncoding, dedupUrl, index, schema, append)

  def convert(pipeFile: String,
              pipeFileEncoding: String,
              deDupBaseUrl: String,     // http://ts10vm.bireme.br:8180/DeDup/services/ or https://dedup.bireme.org/services
              indexName: String,
              schemaName: String,
              append: Boolean): Unit = {
    // Verifying pipe file integrity
    println("\nVerifying pipe file integrity")
    val goodFileName = File.createTempFile("good", "").getPath
    val badFileName = File.createTempFile("bad", "").getPath
    val (good,bad) = VerifyPipeFile.checkRemote(pipeFile, pipeFileEncoding,
          deDupBaseUrl + "/schema/" + schemaName, goodFileName, badFileName)

    println(s"Using $good documents")
    if (good == 0) println(s"Probably error during the check using the remote schema [$schemaName]")
    else {
      if (bad > 0) println(s"Skipping $bad documents. See file: $badFileName\n")

      if (!append) resetIndex(deDupBaseUrl, indexName)

      val quantity = 1000 // Number of documents sent to each call of DeDup service
      val src = Source.fromFile(goodFileName, "utf-8")
      var cur = 0

      new LineBatchIterator(src.getLines(), quantity).foreach {
        batch =>
          println(s"<<< $cur")
          rsend(deDupBaseUrl, indexName, schemaName, batch)
          cur += quantity
      }
      src.close()
    }
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

    val response: HttpResponse[String] = Http(resetUrl).header("content-type", "text/plain")
                                    .option(HttpOptions.followRedirects(true)).charset("utf-8").asString
    if (response.code == 200) {
      val content = response.body
      if (content.startsWith("ERROR:")) throw new IOException(content)
    } else throw new IOException(s"status code:${response.code}")
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
    val response: HttpResponse[String] = Http(burl + "putDocs/" + "/" + indexName + "/" + schemaName)
      .header("content-type", "text/plain")
      .option(HttpOptions.followRedirects(true))
      .charset("utf-8")
      .postData(lines)
      .asString
    val statusCode = response.code
    val ret = if (statusCode == 200) {
      val content = response.body
      if (content.startsWith("ERROR:")) throw new IOException(content)
      val respo = optimizeIndex(burl, indexName)
      if (respo.startsWith("ERROR:")) throw new IOException(respo)
      content
    } else throw new IOException(s"status code:$statusCode")
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

    val response: HttpResponse[String] = Http(burl + "optimize/" + "/" + indexName)
      .header("content-type", "text/plain")
      .option(HttpOptions.followRedirects(true))
      .charset("utf-8")
      .timeout(connTimeoutMs = 5 * 60000, readTimeoutMs = 5 * 60000)
      .asString

    val statusCode = response.code
    val ret = if (statusCode == 200) {
      val content = response.body
      if (content.startsWith("ERROR:")) throw new IOException(content)
      content
    } else throw new IOException(s"status code:$statusCode")
    ret
  }
}

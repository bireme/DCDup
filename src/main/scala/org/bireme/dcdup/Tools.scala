/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io.IOException
import java.nio.charset.Charset
import java.text.Normalizer
import java.text.Normalizer.Form
import java.util
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils

object Tools {
  def isUtf8Encoding(text: String): Boolean = {
    require(text != null)

    val utf8: Charset = Charset.availableCharsets().get("UTF-8")
    val b1: Array[Byte] = text.getBytes(utf8)
    val b2: Array[Byte] = new String(b1, utf8).getBytes(utf8)

    util.Arrays.equals(b1, b2)
  }

  def normalize(in: String): String = Normalizer.normalize(in.trim().toLowerCase, Form.NFD)
    .replaceAll("[^a-z0-9]", "")

  def changeFileExtension(fname: String,
                          extension: String): String = {
    val fnam: String = fname.trim
    val ext: String = extension.trim
    val ext1: String = if (ext.isEmpty) ext
    else if (ext(0) == '.') ext else s".$ext"

    if (fnam.contains('.')) fnam.replaceAll("\\.[^.]*$", ext1)
    else s"$fnam$ext1"
  }

  /**
    * Loads a schema file from the DeDup server
    *
    * @param baseUrl url to DeDup webservice, usually http://dedup.bireme.org/services
    * @param schemaName DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @return the schema as a String
    */
  def loadSchema(baseUrl: String,
                 schemaName: String): String = {
    val baseUrlTrim: String = baseUrl.trim
    val burl: String = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
    val schemaUrl: String = burl + "schema/xml/" +  schemaName
    val httpClient: CloseableHttpClient = HttpClientBuilder.create().build()
    val get: HttpGet = new HttpGet(schemaUrl)

    get.setHeader("Content-type", "text/plain;charset=utf-8")
    val response: CloseableHttpResponse = httpClient.execute(get)
    val statusCode: Int = response.getStatusLine.getStatusCode
    val ret: String = if (statusCode == 200) {
      val content: String = EntityUtils.toString(response.getEntity)
      if (content.startsWith("ERROR:")) throw new IOException(content)
      content
    } else throw new IOException(s"url=$schemaUrl statusCode=$statusCode")

    httpClient.close()
    ret
  }
}

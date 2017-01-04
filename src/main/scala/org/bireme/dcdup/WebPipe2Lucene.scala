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

import java.io.{File,IOException}

import scala.io.Source

import org.apache.http.{HttpEntity,HttpResponse}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils

object WebPipe2Lucene extends App {
  private def usage(): Unit = {
    System.err.println("usage: WebPipe2Lucene " +
      "\n\t<pipeFile> - DeDup piped input file" +
      "\n\t<pipeFileEncoding> - pipe file character encoding" +
      "\n\t<DeDupBaseUrl> - DeDup url service" +
      "\n\t<indexName> - DeDup index Name" +
      "\n\t<schemaName> - DeDup schema name")
    System.exit(1)
  }

  if (args.length < 5) usage()

  convert(args(0), args(1), args(2), args(3), args(4))

  def convert(pipeFile: String,
              pipeFileEncoding: String,
              deDupBaseUrl: String,     // http://ts10vm.bireme.br:8180/DeDup/services/ or http://dedup.bireme.org/services
              indexName: String,
              schemaName: String): Unit = {
    val quantity = 250 // Number of documents sent to each call of DeDup service
    val src = Source.fromFile(pipeFile, pipeFileEncoding)
    var cur = 0

    new LineBatchIterator(src.getLines(), quantity).foreach {
      batch =>
        println(s"<<< $cur")
        rsend(deDupBaseUrl, indexName, schemaName, batch)
        cur += quantity
    }
    src.close()
  }

  /** Checks some documents via DeDup webservice to look for similar docs.
    *
    * @param baseUrl DeDup service url. For example: http://dedup.bireme.org/services or http://ts10vm.bireme.br:8180/services
    * @param indexName DeDup index name
    * @param schemaName DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param lines string having documents separated by new line character
    * @return a json string having the duplicated documents
    */
  private def rsend(baseUrl: String,
                    indexName: String,
                    schemaName: String,
                    lines: String): String = {
    val baseUrlTrim = baseUrl.trim
    val burl = if (baseUrlTrim.endsWith("/")) baseUrlTrim else baseUrlTrim + "/"
    val httpClient = HttpClientBuilder.create().build()
    val post = new HttpPost(burl + "putDocs/" + "/" + indexName + "/" + schemaName)
    val postingString = new StringEntity(lines)

    post.setEntity(postingString)
    post.setHeader("Content-type", "text/plain;charset=utf-8")
    val response = httpClient.execute(post)
    val statusCode = response.getStatusLine.getStatusCode
    val ret = if (statusCode == 200) {
      val content = EntityUtils.toString(response.getEntity())
      if (content.startsWith("ERROR:")) throw new IOException(content)
      content
    } else throw new IOException(s"status code:$statusCode")

    httpClient.close()
    ret
  }
}

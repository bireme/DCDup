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

package org.bireme.sd

import java.net.{URL,URI}
import org.scalatest._
import org.scalatest.concurrent.Timeouts._
import org.scalatest.Matchers._
import org.scalatest.time.SpanSugar._
import scala.io._

/** Application which uses ScalaTest to check each function from DeDup Service
*
* @author: Heitor Barbieri
* date: 20170724
*/
class DeDupServiceTest extends FlatSpec {

  /**
    * Load the content of a web page and check if there is a Timeouts
    *
    * @param url the address to the page to be downloaded
    * @return the page content
    */
  private def pageContent(url:String): String = {
    require(url != null)

    val url2= new URL(url);
    val uri = new URI(url2.getProtocol(), url2.getUserInfo(), url2.getHost(),
                     url2.getPort(), url2.getPath(), url2.getQuery(), url2.getRef())
    val urlStr = uri.toASCIIString()

    var content = ""
    failAfter(60 seconds) {
      val source = Source.fromURL(urlStr, "utf-8")
      content = source.getLines().mkString("\n")
      source.close()
    }

    content
  }

  val service = "http://dedup.bireme.org"

  // === Check if the server is accessible ===
  "The DeDup Service page" should "be on" in {
    val title = "<title>([^<]+)</title>".r
    val url = s"$service"
    val content = pageContent(url)

    title.findFirstMatchIn(content) match {
      case Some(mat) => mat.group(1) should be ("DeDup - Finding duplicated records")
      case None => fail
    }
  }

  // === Check if the Similar Documents applet is available ===
  "The Similar Documents Service servlet" should "be on" in {
    val opt = "\\<option value=\"lilacs_Sas\"(\\s+selected)?\\s+\\>lilacs_Sas\\</option\\>".r
    val url = s"$service/services"
    val content = pageContent(url)

    content should include regex opt
  }

  // === Check the 'Show Schemas' service ===
  "The user" should "retrieve all DeDup schemas" in {
  val url = "http://dedup.bireme.org/services/schemas"
  val content = pageContent(url)

  content should include regex "LILACS_Sas_Seven"
  }

  // === Check the 'Show Indexes' service ===
  "The user" should "retrieve all DeDup indexes" in {
    val url = "http://dedup.bireme.org/services/indexes"
    val content = pageContent(url)

    content should include regex "lilacs_Sas"
  }

  // === Check the 'Show One Schema' service ===
  "The user" should "retrieve 'LILACS_Sas_Seven' schema" in {
    val url = "http://dedup.bireme.org/services/schema/LILACS_Sas_Seven"
    val content = pageContent(url)

    content should include regex "\"name\":\"LILACS_Sas_Seven\""
  }

  // === Check the "Duplicates (GET)" service ===
  "The user" should "retrieve similar documents" in {
    val url = "http://dedup.bireme.org/services/get/duplicates/" +
    "?database=lilacs_Sas" +
    "&schema=LILACS_Sas_Seven" +
    "&quantity=1" +
    "&id=1" +
    "&titulo_artigo=Prevalência da perda de primeiros molares permanentes, em escolares de 6 a 12 anos, de ambos os sexos, da cidade de Ribeirão Preto (SP)" +
    "&titulo_revista=Rev. odontol. Univ. São Paulo" +
    "&ano_publicacao=1989" +
    "&volume_fasciculo=3" +
    "&numero_fasciculo=1" +
    "&autores=Ferlin, Lúcia Helena Mian//@//Daruge, Angelica Dolcimascolo//@//Daruge, Rudiney Jefferson//@//Rancan, Sandra Valéria" +
    "&pagina_inicial=239"

    val content = pageContent(url)
    val similarity = "\"similarity\":\"([^\"]+)\"".r
    val simVal = similarity.findFirstMatchIn(content) match {
      case Some(mat) => mat.group(1).toFloat
      case None => 0f
    }
    simVal should be >= 0.8f
  }
}

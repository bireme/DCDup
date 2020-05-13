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

package org.bireme.dcdup

import java.net.{URI, URL}

import org.scalatest.flatspec.AnyFlatSpec

import Ordering.Float.TotalOrdering  // scala 2.13.0

import org.scalatest.concurrent.TimeLimits._
import org.scalatest.time.SpanSugar._
import org.scalatest.matchers.should.Matchers
import Matchers._
import scalaj.http.{Http, HttpRequest}

import scala.io._
import scala.util.matching.Regex

/** Application which uses ScalaTest to check each function from DeDup Service
*
* author: Heitor Barbieri
* date: 20170724
*/
class DeDupServiceTest extends AnyFlatSpec {

  /**
    * Load the content of a web page and check if there is a Timeout
    *
    * @param url the address to the page to be downloaded
    * @return the page content
    */
  private def pageContent(url:String): String = {
    require(url != null)

    val url2: URL = new URL(url)
    val uri: URI = new URI(url2.getProtocol, url2.getUserInfo, url2.getHost,
                     url2.getPort, url2.getPath, url2.getQuery, url2.getRef)
    val urlStr: String = uri.toASCIIString

    failAfter(timeout = 60.seconds) {
      val source: BufferedSource = Source.fromURL(urlStr, "utf-8")
      val content: String = source.getLines().mkString("\n")

      source.close()
      content
    }
  }

  val service = /*"http://localhost:8084/DeDup"*/ /*"http://serverofi5.bireme.br:8180/DeDup"*/ "https://dedup.bireme.org"

  // === Check if the server is accessible ===
  "The DeDup Service page" should "be on" in {
    val title: Regex = "<title>([^<]+)</title>".r
    val url: String = s"$service"
    val content: String = pageContent(url)

    title.findFirstMatchIn(content) match {
      case Some(mat) => mat.group(1) should be ("DeDup - Finding duplicated records")
      case None => fail
    }
  }

  // === Check if the Similar Documents applet is available ===
  "The Similar Documents Service servlet" should "be on" in {
    val opt: Regex = "<option value=\"lilacs_Sas\"(\\s+selected)?\\s+>lilacs_Sas</option>".r
    val url: String = s"$service/services"
    val content: String = pageContent(url)

    content should include regex opt
  }

  // === Check the 'Show Schemas' service ===
  "The user" should "retrieve all DeDup schemas" in {
  val url: String = s"$service/services/schemas"
  val content: String = pageContent(url)

  content should include regex "LILACS_Sas_Seven"
  }

  // === Check the 'Show Indexes' service ===
  "The user" should "retrieve all DeDup indexes" in {
    val url: String = s"$service/services/indexes"
    val content: String = pageContent(url)

    content should include regex "lilacs_Sas"
  }

  // === Check the 'Show One Schema' service ===
  "The user" should "retrieve 'LILACS_Sas_Seven' schema" in {
    val url: String = s"$service/services/schema/LILACS_Sas_Seven"
    val content: String = pageContent(url)

    content should include regex "\"name\":\"LILACS_Sas_Seven\""
  }

  // === Check the "Duplicates (GET)" service ===
  "The user" should "retrieve similar documents" in {
    val url: String = s"$service/services/get/duplicates/" +
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

    val content: String = pageContent(url)
    val similarity: Regex = "\"similarity\":\"([^\"]+)\"".r
    val simVal: Float = similarity.findFirstMatchIn(content) match {
      case Some(mat) => mat.group(1).toFloat
      case None => 0f
    }
    simVal should be >= 0.8f
  }

  // === Check the delete document service - preambule ===
  "The user" should "delete the fake document before insert fakes documents" in {
    val url = s"$service/services/delete" +
      "?database=lilacs_Sas" +
      "&id=fake*"

    val content: String = pageContent(url)

    content should include regex "OK"
  }

  // === Check the insertion of one fake document (wrong version) ===
  "The user" should "be able to insert a fake document (wrong version)" in {
    val http = Http(s"$service/services/raw/put/lilacs_Sas/LILACS_Sas_Seven")
    val parameters: String =
      "lilacs_Sas|fake|Como eu sou uma pessoa maravilhosa.|Verdades Universais|2019|1|1|Heitor Barbiere|15"

    val content: String = http.postData(parameters).header("content-type", "text/plain;charset=utf-8").asString.body

    content shouldBe "OK"
  }

  // === Check the insertion of one fake document (right version) ===
  "The user" should "be able to insert a fake document (right version)" in {
    val http = Http(s"$service/services/raw/put/lilacs_Sas/LILACS_Sas_Seven")
    val parameters =
      "lilacs_Sas|fake|Como eu sou uma pessoa maravilhosa.|Verdades Universais|2019|1|1|Heitor Barbieri|15"

    val content: String = http.postData(parameters).header("content-type", "text/plain;charset=utf-8").asString.body

    content shouldBe "OK"
  }

  // === Check the "Duplicates (GET)" service ===
  "The user" should "retrieve similar documents of the fake document" in {
    val url: String = s"$service/services/get/duplicates/" +
      "?database=lilacs_Sas" +
      "&schema=LILACS_Sas_Seven" +
      "&quantity=2" +
      "&id=1" +
      "&titulo_artigo=Como eu sou uma pessoa maravilhosa." +
      "&titulo_revista=Verdades Universais" +
      "&ano_publicacao=2019" +
      "&volume_fasciculo=1" +
      "&numero_fasciculo=1" +
      "&autores=Heitor Barbier" +
      "&pagina_inicial=15"


    val content: String = pageContent(url)
    val similarity: Regex = "\"similarity\":\"([^\"]+)\"".r
    val total: Regex = ",\"total\":(\\d+),".r
    val simVal: Float = similarity.findFirstMatchIn(content) match {
      case Some(mat) => mat.group(1).toFloat
      case None => 0f
    }
    val totVal: Int = total.findFirstMatchIn(content) match {
      case Some(tot) => tot.group(1).toInt
      case None => 0
    }

    simVal shouldBe 1.0f
    content should include regex "Barbier."
    totVal shouldBe 1  // Multiples insert with same id always will replace documents

  }

  // === Check the delete document service ===
  "The user" should "delete the fake document" in {
    val url = s"$service/services/delete" +
      "?database=lilacs_Sas" +
      "&id=fake"

    val content: String = pageContent(url)

    content should include regex "OK"
  }

  // === Check the "Duplicates (GET)" service ===
  "The user" should "not retrieve similar documents of the fake document" in {
    val url = s"$service/services/get/duplicates/" +
      "?database=lilacs_Sas" +
      "&schema=LILACS_Sas_Three" +
      "&quantity=1" +
      "&id=1" +
      "&titulo_artigo=Como eu sou uma pessoa maravilhosa." +
      "&titulo_revista=Verdades Universais" +
      "&ano_publicacao=2019"

    val content: String = pageContent(url)
    val similarity: Regex = "\"similarity\":\"([^\"]+)\"".r
    val found: Boolean = similarity.findFirstMatchIn(content).isDefined

    found shouldBe false
  }

  // === Check the insertion of one fake document (Portuguese version) ===
  "The user" should "be able to insert a fake document (Portuguese)" in {
    val http: HttpRequest = Http(s"$service/services/raw/put/lilacs_Sas/LILACS_Sas_Seven")
    val parameters: String =
      "lilacs_Sas|fake-pt|Sou um ser iluminado e maravilhoso.|Verdades Universais|2019|1|1|Heitor Barbieri|15"

    val content: String = http.postData(parameters).header("content-type", "text/plain;charset=utf-8").asString.body

    content shouldBe "OK"
  }

  // === Check the insertion of one fake document (Spanish version) ===
  "The user" should "be able to insert a fake document (English)" in {
    val http: HttpRequest = Http(s"$service/services/raw/put/lilacs_Sas/LILACS_Sas_Seven")
    val parameters: String =
      "lilacs_Sas|fake-es|Soy un ser iluminado y maravilloso.|Verdades Universais|2019|1|1|Heitor Barbieri|15"

    val content: String = http.postData(parameters).header("content-type", "text/plain;charset=utf-8").asString.body

    content shouldBe "OK"
  }

  // === Check the "Duplicates (GET)" service (pt/en) ===
  "The user" should "retrieve similar documents of the fake document (pt/en)" in {
    val url: String = s"$service/services/get/duplicates/" +
      "?database=lilacs_Sas" +
      "&schema=LILACS_Sas_Seven" +
      "&quantity=2" +
      "&id=1" +
      "&titulo_artigo=Sou um ser iluminado e maravilhoso." +
      "&titulo_revista=Verdades Universais" +
      "&ano_publicacao=2019" +
      "&volume_fasciculo=1" +
      "&numero_fasciculo=1" +
      "&autores=Heitor Barbier" +
      "&pagina_inicial=15"


    val content: String = pageContent(url)
    val similarity: Regex = "\"similarity\":\"([^\"]+)\"".r
    val total: Regex = ",\"total\":(\\d+),".r
    val simVal: Float = similarity.findFirstMatchIn(content) match {
      case Some(mat) => mat.group(1).toFloat
      case None => 0f
    }
    val totVal: Int = total.findFirstMatchIn(content) match {
      case Some(tot) => tot.group(1).toInt
      case None => 0
    }

    simVal should be >= 0.7f
    content should include regex "Barbier."
    totVal shouldBe 2
  }

  // === Check the delete document service (multiple documents) ===
  "The user" should "delete the fake documents" in {
    val url = s"$service/services/delete" +
      "?database=lilacs_Sas" +
      "&id=fake*"

    val content: String = pageContent(url)

    content should include regex "OK"
  }

  // === Check the "Duplicates (GET)" service (multiple documents) ===
  "The user" should "not retrieve similar documents of the fake documents" in {
    val url: String = s"$service/services/get/duplicates/" +
      "?database=lilacs_Sas" +
      "&schema=LILACS_Sas_Three" +
      "&quantity=1" +
      "&id=1" +
      "&titulo_artigo=Sou um ser iluminado e maravilhoso." +
      "&titulo_revista=Verdades Universais" +
      "&ano_publicacao=2019"

    val content: String = pageContent(url)
    val similarity: Regex = "\"similarity\":\"([^\"]+)\"".r
    val found: Boolean = similarity.findFirstMatchIn(content).isDefined

    found shouldBe false
  }
}

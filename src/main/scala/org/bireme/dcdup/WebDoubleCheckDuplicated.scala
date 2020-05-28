/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import java.io._
import java.nio.charset.Charset
import java.nio.file.{Files, Paths, StandardOpenOption}

import br.bireme.ngrams.NGSchema
import org.apache.http.client.methods.HttpPost
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
      "\n\t-pipe=<pipeFile> - DeDup piped input file" +
      "\n\t-dedupUrl=<DeDupBaseUrl> - DeDup url service. For ex: http://dedup.bireme.org/services" +
      "\n\t-index=<indexName> - DeDup index name" +
      "\n\t-schema=<schemaName> - DeDup schema name" +
      "\n\t-outDupFile1=<outDupFile1> - duplicated records found in pipe file" +
      "\n\t-outDupFile2=<outDupFile2> - duplicated records found between pipe file and Dedup index" +
      "\n\t-outNoDupFile1=<outNoDupFile1> - no duplicated records between input pipe file and Dedup index" +
      "\n\t-outNoDupFile2=<outNoDupFile2> - no duplicated records between (pipe file and itself) and (pipe file and Dedup index)" +
      "\n\t[-pipeEncoding=<pipeFileEncoding>] - pipe file character encoding. Default is utf-8")
    System.exit(1)
  }

  if (args.length < 8) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
  }
  val pipe = parameters("pipe")
  val dedupUrl = parameters("dedupUrl")
  val index = parameters("index")
  val schema = parameters("schema")
  val outDupFile1 = parameters("outDupFile1")
  val outDupFile2 = parameters("outDupFile2")
  val outNoDupFile1 = parameters("outNoDupFile1")
  val outNoDupFile2 = parameters("outNoDupFile2")
  val pipeEncoding = parameters.getOrElse("pipeEncoding", "utf-8")

  val dup = new WebDoubleCheckDuplicated()
  dup.doubleCheck(pipe, pipeEncoding, dedupUrl, index, schema,
                  outDupFile1, outDupFile2, outNoDupFile1, outNoDupFile2)
}

/** Class to check a DeDup input piped file against itself to look for duplicated
  * documents and then against a Dedup index via DeDup webservice
  * to also look for duplicated docs.
  *
  * author: Heitor Barbieri
  * date: 20161208
*/
class WebDoubleCheckDuplicated {
  def doubleCheck(pipe: String,
                  pipeEncoding: String,
                  deDupBaseUrl: String, // http://ts10vm.bireme.br:8180/DeDup/services/ or http://dedup.bireme.org/services
                  indexName: String, // Verifying pipe file integrity
                  schemaName: String, // used in DeDup service
                  outDupFile1: String,
                  outDupFile2: String,
                  outNoDupFile1: String,
                  outNoDupFile2: String): Unit = {
    // Check pipe file
    println("\nChecking pipe file ...")
    val (_, bad, goodFile, badFile) = {
      val schemaUrl: String = s"${deDupBaseUrl.trim}/schema/$schemaName"
      checkPipeFile(pipe, pipeEncoding, schemaUrl)
    }
    if (bad > 0) println(s"\nSkipping $bad bad lines from pipe file. See file: $badFile")

    val schemaStr = Tools.loadSchema(deDupBaseUrl, schemaName)
//println(s"[$schemaStr]")
    val ngSchema = new NGSchema(schemaName, schemaStr)

    // Self check
    println("\n***Self check")
    CheckDuplicated.checkDuplicated(goodFile, "utf-8" , None, ngSchema, outDupFile1, outNoDupFile1 + "_self")

    // Check using DeDup service
    println("\n***Remote check")
    remoteCheck(deDupBaseUrl, indexName, schemaName, goodFile, outDupFile2 + "_tmp")

    print("OK\nPost processing remote duplicated files ... ")
    val dupIds: Map[String, Set[String]] =
      CheckDuplicated.postProcessDup(outDupFile2 + "_tmp", outDupFile2)

    print("OK\n\nPost processing no duplicated remote files ... ")
    val idsDup: Set[String] = dupIds.foldLeft(Set[String]()) ((set, kv) => set ++ (kv._2 + kv._1))
    CheckDuplicated.postProcessNoDup(goodFile, "utf-8", ngSchema,
                                     outNoDupFile1 + "_remote", idsDup)

    print("OK\n\nRemoving duplicated lines ... ")

    // Take duplicate no duplicated documents between input pipe file and Dedup index
    CheckDuplicated.takeNoDuplicatedLight(ngSchema, outNoDupFile1 + "_self", outNoDupFile1 + "_remote",
      outNoDupFile1)

    // Take duplicate no duplicated documents between (pipe file and itself) and (pipe file and Dedup index)
    CheckDuplicated.takeNoDuplicated(ngSchema, outNoDupFile1 + "_self", outNoDupFile1 + "_remote",
      outNoDupFile2)

    println("OK")

    // Delete pre-processed output files
    CheckDuplicated.deleteFile(new File(outDupFile2 + "_tmp"))
    CheckDuplicated.deleteFile(new File(outNoDupFile1 + "_self"))
    CheckDuplicated.deleteFile(new File(outNoDupFile1 + "_remote"))
    CheckDuplicated.deleteFile(new File(goodFile))
  }

  /** Given an input piped file and a NGrams index, looks for documents that
    * are in the input file that are duplicated to the ones stored in the index
    * through DeDup web service.
    *
    * @param deDupBaseUrl url to DeDup webservice, usually http://dedup.bireme.org/services
    * @param indexName DeDup index name used to look for duplicates. See http://dedup.bireme.org/services/indexes
    * @param schemaName DeDup data schema name. See http://dedup.bireme.org/services/schemas
    * @param pipeFile input piped file used to look for similar docs
    * @param outDupFile output piped file with the duplicated documents
    */
  private def remoteCheck(deDupBaseUrl: String,
                          indexName: String,
                          schemaName: String,
                          pipeFile: String,
                          outDupFile: String): Unit = {
    val quantity = 250 // Number of documents sent to each call of DeDup service
    val src = Source.fromFile(pipeFile, "utf-8")
    val dest = Files.newBufferedWriter(Paths.get(outDupFile), Charset.forName("utf-8"),
                                       StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
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

  /**
  * Check an input pipe file against a DeDup schema
    * @param pipe input file
    * @param encoding input file encoding
    * @param schemaUrl url of the DeDup schema
    * @return (number of good input lines, number of incorrect input lines,
    *          path to good output file, path to bad output file)
    */
  private def checkPipeFile(pipe: String,
                            encoding: String,
                            schemaUrl: String): (Int, Int, String, String) = {
    val good: File = File.createTempFile("check_good", null)
    val bad: File = File.createTempFile("check_bad", null)
    val (numGood, numBad) = VerifyPipeFile.checkRemote(pipe, encoding, schemaUrl, good.getPath, bad.getPath)

    (numGood, numBad, good.getPath, bad.getPath)
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
//println(s"@lines=[$lines]")
//println(s"@lines=[$lines]")
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
}

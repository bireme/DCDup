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

import java.io.File
import java.nio.file.{Files,Paths}
import java.nio.charset.{Charset, CodingErrorAction}

import scala.io.{Codec, Source}

/**
  * Create a Lucene index from an input piped file and a duplicated output file
  *
  */
object Duplicated2Lucene extends App {
  private def usage(): Unit = {
    System.err.println("usage: Duplicated2Lucene" +
      "\n\t-pipeFile=<pipeFile> - pipe file used in NGram duplication process" +
      "\n\t[-pipeFileEncoding=<pipeEncoding>] - pipe file encoding. Default is utf-8" +
      "\n\t-dupFile=<dupPipeFile> - duplicated pipe file (output file of duplication check)" +
      "\n\t[-dupFileEncoding=<pipeEncoding>] - pipe file encoding. Default is utf-8" +
      "\n\t-indexPath=<indexPath> - NGram's Lucene index path" +
      "\n\t-schemaFile=<schemaFile> - NGram schema file" +
      "\n\t[-schemaFileEncoding=<schemaEncoding>] - NGram schema file encoding"
    )
    System.exit(1)
  }

  if (args.length < 4) usage()

  val parameters = args.foldLeft[Map[String,String]](Map()) {
    case (map,par) => {
      val split = par.split(" *= *", 2)
      if (split.length == 1) map + ((split(0).substring(2), ""))
      else map + ((split(0).substring(1), split(1)))
    }
  }

  val pipeFile = parameters("pipeFile")
  val pipeFileEncoding = parameters.getOrElse("pipeFileEncoding", "utf-8")
  val dupFile = parameters("dupFile")
  val dupFileEncoding = parameters.getOrElse("dupFileEncoding", "utf-8")
  val indexPath = parameters("indexPath")
  val schemaFile = parameters("schemaFile")
  val schemaFileEncoding = parameters.getOrElse("schemaFileEncoding", "utf-8")

  val denied = getDeniedDocIds(dupFile, dupFileEncoding)
  val noDupPipeFile = genNoDupFile(pipeFile, pipeFileEncoding, denied)
  val p2l = new Pipe2Lucene()
  p2l.toLucene(indexPath, schemaFile, schemaFileEncoding, noDupPipeFile,
                 "utf-8", false)

  new File(noDupPipeFile).delete()

  /**
    * Create a set of document id that should not be included into the index
    *
    * @param dupFile output file of the deduplication process
    * @param dupFileEncoding output file encoding
    * @return  a set of document ids that should not be included into the  Lucene index
    */
  private def getDeniedDocIds(dupFile: String,
                              dupFileEncoding: String): Set[Int] = {
    val codec = dupFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val dsrc = Source.fromFile(dupFile)(decoder)

    val set = dsrc.getLines.foldLeft[Set[Int]](Set()) {
      case (set1,line) =>
        val tline = line.trim()
        if (!tline.isEmpty()) {
          val split = tline.split("\\|")
          if (split.size >= 4) {
            val v1 = split(2).toInt
            val v2 = split(3).toInt
            if (v1 != v2) set1 + Math.max(v1, v2) else set1
          } else set1
        } else set1
    }
    dsrc.close()
    set
  }

  /**
    * Create a pipe file having no duplicated documents
    *
    * @param pipeFile original pipe file
    * @param pipeFileEncoding original pipe file encoding
    * @param denied set of document ids that should not be included into indexName
    * @return the name of temporary working file whose dulicated documents are removed
    */
  private def genNoDupFile(pipeFile: String,
                           pipeFileEncoding: String,
                           denied: Set[Int]): String = {
    val codec = pipeFileEncoding.toLowerCase match {
      case "iso8859-1" => Codec.ISO8859
      case _           => Codec.UTF8
    }
    val codAction = CodingErrorAction.REPLACE
    val decoder = codec.decoder.onMalformedInput(codAction)
    val src = Source.fromFile(pipeFile)(decoder)
    val tmpFile = Files.createTempFile(Paths.get("."), null, null)
    val writer = Files.newBufferedWriter(tmpFile,
                                         Charset.forName(pipeFileEncoding))
    src.getLines().foreach {
      line =>
        val tline = line.trim()
        if (!tline.isEmpty()) {
          val split = tline.split("\\|", 3)
          if (split.size >= 3) {
            val v1 = split(1).toInt
            if (!denied.contains(v1)) writer.write(line + "\n")
          }
        }
    }

    writer.close()
    src.close()
    tmpFile.toString()
  }
}

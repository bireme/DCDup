/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

import scala.collection.immutable.TreeMap
import scala.io.{BufferedSource, Source}

object Statistics extends App {
  private def usage(): Unit = {
    System.err.println("usage: Statistics -pipe=<pipeFile> [-encoding=<encod>] [-separator=<char>]")
    System.exit(1)
  }

  val seq = args.toSeq.filter(_.nonEmpty)
  if (seq.length < 1) usage()

  val parameters = seq.foldLeft[Map[String,String]](Map()) {
    case (map,par) =>
      val split = par.split(" *= *", 2)
      map + ((split(0).trim.substring(1), split(1).trim))
  }
  val keys = parameters.keys.toSet
  if (!keys.contains("pipe")) usage()

  stat(parameters("pipe"),
       parameters.getOrElse("encoding", "utf-8"),
       parameters.getOrElse("separator", "|"))

  private def stat(fname: String,
                   encoding: String,
                   separator: String): Unit = {
    val src: BufferedSource = Source.fromFile(fname, encoding)
    val lines: Iterator[String] = src.getLines()

    val (totLines, colNumber, presenceCols, linePatterns) =
      stat(lines, 0, 0, Map[Int, Int](), TreeMap[String, (Int, Int)](), separator)

    println("\n=============================================================================")
    println(s"File: $fname")
    println(s"Number of lines: $totLines")
    println(s"Number of columns: $colNumber\n")
    for (col <- 1 to colNumber) {
      val presence = presenceCols.getOrElse(col, 0)
      val percent = presence.toFloat/totLines*100
      println(s"Column [$col] used $presence times" + f"[$percent%1.2f" + "%]")
    }

    for (used <- 0 to colNumber) {
      val cnum: Map[String, (Int, Int)] = linePatterns.filter(_._2._1 == used)
      val tot: Int = cnum.values.foldLeft(0) ((t, el) => t + el._2)
      val percent = tot.toFloat / totLines * 100
      val sorted: Map[Int, Seq[String]] = cnum.foldLeft(TreeMap[Int, Seq[String]]()) {
        case (map, (k:String, v: (Int, Int))) =>  map + (v._2 -> (map.getOrElse(v._2, Seq[String]()) prepended k))
      }
      println(s"\nLines with $used used columns: $tot " + f"[$percent%1.2f" + "%]")
      sorted.toSeq.reverse.foreach {
        elem =>
          val percent2 =  elem._1.toFloat / tot * 100
          elem._2.sorted.reverse.foreach {
            pat => println(s"\t$pat ==> ${elem._1} times " + f"[$percent2%1.2f" + "%]")
          }
      }
    }
    println("=============================================================================")

    src.close()
  }

  @scala.annotation.tailrec
  private def stat(lines: Iterator[String],
                   totLines: Int, // total number of lines
                   colNumber: Int, // number of columns
                   presenceCols: Map[Int, Int], // total occurrences of each column
                   linePatterns: Map[String, (Int, Int)],   // line pattern ("11100111", occurrences os 1, total)
                   separator: String
                   ): (Int, Int, Map[Int,Int], Map[String, (Int, Int)]) = {
    if (lines.hasNext) {
      val line: String = lines.next().trim
      if (line.nonEmpty) {
        val split: Array[String] = line.split("\\" + separator, Integer.MAX_VALUE).map(_.trim)
        val slen: Int = split.length
        val colNum = Math.max(colNumber, slen)

        val pCols: Map[Int, Int] = (0 until slen).foldLeft(presenceCols) {
          case (map, col) => if (split(col).trim.isEmpty) map
                             else map + (col -> (map.getOrElse(col, 0) + 1))
        }
        val pattern: String = split.foldLeft("") {
          case (str, elem) => str + (if (elem.isBlank) "0" else "1")
        }
        val ucpLine: Map[String, (Int, Int)] = {
          val tot: Int = split.count(_.nonEmpty)
          val (occ, sum) = linePatterns.getOrElse(pattern, (tot, 0))
          linePatterns + (pattern -> (occ, sum + 1))
        }
        stat(lines, totLines + 1, colNum, pCols, ucpLine, separator)
      } else stat(lines, totLines, colNumber, presenceCols, linePatterns, separator)
    } else (totLines, colNumber, presenceCols, linePatterns)
  }
}

/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

/**
* An iterator that join each step (next()) a number of lines from an external iterator
  * @param iterator the external iterator
  * @param batchSize number of lines joined each step
  * @param ignoreWhiteLines if true skip (do not count) white/empty lines
  */
class LineBatchIterator(iterator: Iterator[String],
                        batchSize: Int,
                        ignoreWhiteLines: Boolean = true) extends
                                                            Iterator[String] {
  var current: String = getBatchLines(iterator, first = true)

  override def hasNext: Boolean = !current.isEmpty

  override def next(): String = {
    val cur = current
    current = getBatchLines(iterator, first = false)
    cur
  }

  /**
  * Read and concatenate the first 'batchSize' lines into a string
    * @param iterator input string iterator
    * @param first tell if it is the first line of the iterator
    * @return a string with the lines concatenated
    */
  private def getBatchLines(iterator: Iterator[String],
                            first: Boolean): String = {
    getLines(iterator, batchSize, "", first)
  }

  /**
  * Read and concatenate the first 'batchSize' lines into a string
 *
    * @param iterator input string iterator
    * @param remLines the number of lines to be read until the batchSize lines
    * @param lines auxiliary buffer
    * @param first tell if it is the first line of the iterator
    * @return a string with the lines concatenated
    */
  @scala.annotation.tailrec
  private def getLines(iterator: Iterator[String],
                       remLines: Int,
                       lines: String,
                       first: Boolean) : String = {
    if ((remLines <= 0) || !iterator.hasNext) lines
    else {
      val line = iterator.next().trim()
      getLines(iterator,
               remLines - 1,
               lines + (
                          if (ignoreWhiteLines && line.isEmpty) ""
                          else if (first) line else s"\n$line"
                        ),
                first = false
              )
    }
  }
}

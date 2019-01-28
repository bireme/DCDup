/*=========================================================================

    DCDup © Pan American Health Organization, 2017.
    See License at: https://github.com/bireme/DCDup/blob/master/LICENSE.txt

  ==========================================================================*/

package org.bireme.dcdup

class LineBatchIterator(iterator: Iterator[String],
                        batchSize: Int,
                        ignoreWhiteLines: Boolean = true) extends
                                                            Iterator[String] {
  var current = getBatchLines(iterator, true)

  override def hasNext: Boolean = !current.isEmpty

  override def next: String = {
    val cur = current
    current = getBatchLines(iterator, false)
    cur
  }

  private def getBatchLines(iterator: Iterator[String],
                            first: Boolean): String = {
    getLines(iterator, batchSize, "", first)
  }

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
                          if ((ignoreWhiteLines) && (line.isEmpty)) ""
                          else if (first) line else s"\n$line"
                        ),
                false
              )
    }
  }
}

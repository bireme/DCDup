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
                          if (ignoreWhiteLines)
                            if (line.isEmpty) ""
                            else if (first) line else s"\n$line"
                          else if (first) line else s"\n$line"
                        ),
                false
              )
    }
  }
}

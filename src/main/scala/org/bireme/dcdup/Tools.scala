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

import java.nio.charset.Charset
import java.util.Arrays

object Tools {
  def isUtf8Encoding(text: String): Boolean = {
    require(text != null)

    val utf8 = Charset.availableCharsets().get("UTF-8")
    val b1 = text.getBytes(utf8)
    val b2 = new String(b1, utf8).getBytes(utf8)

    Arrays.equals(b1, b2)
  }
}

/*
 * Copyright 2017 The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.radarbase.util

/**
 * String utilities.
 */
object Strings {
    private val HEX_ARRAY = "0123456789ABCDEF".toCharArray()

    /**
     * Compiles a pattern that checks if it is contained in another string in a case-insensitive
     * way.
     */
    fun String.toIgnoreCaseRegex(): Regex = toRegex(setOf(
        RegexOption.IGNORE_CASE,
        RegexOption.LITERAL
    ))

    /**
     * Converts given bytes to a hex string.
     * @param bytes bytes to read.
     * @return String with hex values.
     */
    fun ByteArray.toHexString(): String {
        val hexChars = CharArray(size * 2)
        for (i in indices) {
            val value = get(i).toInt() and 0xFF
            hexChars[i * 2] = HEX_ARRAY[value ushr 4]
            hexChars[i * 2 + 1] = HEX_ARRAY[value and 0x0F]
        }
        return String(hexChars)
    }
}

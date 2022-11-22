/*
 * Copyright (c) 2012, 2013, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package org.radarbase.util

// Since Android API 19
/**
 * This class consists exclusively of static methods for obtaining
 * encoders and decoders for the Base64 encoding scheme. The
 * implementation of this class supports the following types of Base64
 * as specified in
 * [RFC 4648](http://www.ietf.org/rfc/rfc4648.txt) and
 * [RFC 2045](http://www.ietf.org/rfc/rfc2045.txt).
 *
 *
 * Uses "The Base64 Alphabet" as specified in Table 1 of
 * RFC 4648 and RFC 2045 for encoding and decoding operation.
 * The encoder does not add any line feed (line separator)
 * character. The decoder rejects data that contains characters
 * outside the base64 alphabet.
 *
 *
 * Unless otherwise noted, passing a `null` argument to a
 * method of this class will cause a [ NullPointerException][java.lang.NullPointerException] to be thrown.
 *
 * Note: needed because it is only included in Android API level 26.
 *
 * @author  Xueming Shen
 * @since   1.8
 */
object Base64Encoder {
    /**
     * This class implements an encoder for encoding byte data using
     * the Base64 encoding scheme as specified in RFC 4648 and RFC 2045.
     *
     *
     * Instances of [Encoder] class are safe for use by
     * multiple concurrent threads.
     *
     *
     * Unless otherwise noted, passing a `null` argument to
     * a method of this class will cause a
     * [NullPointerException][java.lang.NullPointerException] to
     * be thrown.
     *
     * Encodes all bytes from the specified byte array into a newly-allocated
     * byte array using the [Base64] encoding scheme. The returned byte
     * array is of the length of the resulting bytes.
     *
     * @param   src
     * the byte array to encode
     * @return  A newly-allocated byte array containing the resulting
     * encoded bytes.
     */
    fun encode(src: ByteArray): String {
        val srcLen = src.size
        val dst = ByteArray(4 * ((srcLen + 2) / 3))
        val fullDataLen = srcLen / 3 * 3
        var dstP = 0
        var srcP = 0
        while (srcP < fullDataLen) {
            val bits = (src[srcP].toInt() and 0xff).shl(16) or
                    (src[srcP + 1].toInt() and 0xff).shl(8) or
                    (src[srcP + 2].toInt() and 0xff)
            dst[dstP++] = BASE_64_CHAR[bits.ushr(18) and 0x3f]
            dst[dstP++] = BASE_64_CHAR[bits.ushr(12) and 0x3f]
            dst[dstP++] = BASE_64_CHAR[bits.ushr(6) and 0x3f]
            dst[dstP++] = BASE_64_CHAR[bits and 0x3f]
            srcP += 3
        }
        if (srcP < srcLen) {               // 1 or 2 leftover bytes
            val b0 = src[srcP++].toInt() and 0xff
            dst[dstP++] = BASE_64_CHAR[b0 shr 2]
            if (srcP == srcLen) {
                dst[dstP++] = BASE_64_CHAR[b0 shl 4 and 0x3f]
                dst[dstP++] = '='.code.toByte()
            } else {
                val b1 = src[srcP].toInt() and 0xff
                dst[dstP++] = BASE_64_CHAR[b0 shl 4 and 0x3f or (b1 shr 4)]
                dst[dstP++] = BASE_64_CHAR[b1 shl 2 and 0x3f]
            }
            dst[dstP] = '='.code.toByte()
        }
        return String(dst)
    }

    /**
     * This array is a lookup table that translates 6-bit positive integer
     * index values into their "Base64 Alphabet" equivalents as specified
     * in "Table 1: The Base64 Alphabet" of RFC 2045 (and RFC 4648).
     */
    private val BASE_64_CHAR = byteArrayOf(
        'A'.code.toByte(),
        'B'.code.toByte(),
        'C'.code.toByte(),
        'D'.code.toByte(),
        'E'.code.toByte(),
        'F'.code.toByte(),
        'G'.code.toByte(),
        'H'.code.toByte(),
        'I'.code.toByte(),
        'J'.code.toByte(),
        'K'.code.toByte(),
        'L'.code.toByte(),
        'M'.code.toByte(),
        'N'.code.toByte(),
        'O'.code.toByte(),
        'P'.code.toByte(),
        'Q'.code.toByte(),
        'R'.code.toByte(),
        'S'.code.toByte(),
        'T'.code.toByte(),
        'U'.code.toByte(),
        'V'.code.toByte(),
        'W'.code.toByte(),
        'X'.code.toByte(),
        'Y'.code.toByte(),
        'Z'.code.toByte(),
        'a'.code.toByte(),
        'b'.code.toByte(),
        'c'.code.toByte(),
        'd'.code.toByte(),
        'e'.code.toByte(),
        'f'.code.toByte(),
        'g'.code.toByte(),
        'h'.code.toByte(),
        'i'.code.toByte(),
        'j'.code.toByte(),
        'k'.code.toByte(),
        'l'.code.toByte(),
        'm'.code.toByte(),
        'n'.code.toByte(),
        'o'.code.toByte(),
        'p'.code.toByte(),
        'q'.code.toByte(),
        'r'.code.toByte(),
        's'.code.toByte(),
        't'.code.toByte(),
        'u'.code.toByte(),
        'v'.code.toByte(),
        'w'.code.toByte(),
        'x'.code.toByte(),
        'y'.code.toByte(),
        'z'.code.toByte(),
        '0'.code.toByte(),
        '1'.code.toByte(),
        '2'.code.toByte(),
        '3'.code.toByte(),
        '4'.code.toByte(),
        '5'.code.toByte(),
        '6'.code.toByte(),
        '7'.code.toByte(),
        '8'.code.toByte(),
        '9'.code.toByte(),
        '+'.code.toByte(),
        '/'.code.toByte(),
    )
}

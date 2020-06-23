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

package org.radarbase.util;

/**
 * This class consists exclusively of static methods for obtaining
 * encoders and decoders for the Base64 encoding scheme. The
 * implementation of this class supports the following types of Base64
 * as specified in
 * <a href="http://www.ietf.org/rfc/rfc4648.txt">RFC 4648</a> and
 * <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045</a>.
 *
 * <p>Uses "The Base64 Alphabet" as specified in Table 1 of
 *     RFC 4648 and RFC 2045 for encoding and decoding operation.
 *     The encoder does not add any line feed (line separator)
 *     character. The decoder rejects data that contains characters
 *     outside the base64 alphabet.</p>
 *
 * <p>Unless otherwise noted, passing a {@code null} argument to a
 * method of this class will cause a {@link java.lang.NullPointerException
 * NullPointerException} to be thrown.
 *
 * @author  Xueming Shen
 * @since   1.8
 */

@SuppressWarnings("PMD.ClassNamingConventions")
public class Base64 {

    private Base64() {}

    /**
     * Returns a {@link Encoder} that encodes using the
     * <a href="#basic">Basic</a> type base64 encoding scheme.
     *
     * @return  A Base64 encoder.
     */
    public static Encoder getEncoder() {
        return Encoder.RFC4648;
    }

    /**
     * This class implements an encoder for encoding byte data using
     * the Base64 encoding scheme as specified in RFC 4648 and RFC 2045.
     *
     * <p>Instances of {@link Encoder} class are safe for use by
     * multiple concurrent threads.
     *
     * <p>Unless otherwise noted, passing a {@code null} argument to
     * a method of this class will cause a
     * {@link java.lang.NullPointerException NullPointerException} to
     * be thrown.
     *
     * @since   1.8
     */
    public static class Encoder {
        /**
         * This array is a lookup table that translates 6-bit positive integer
         * index values into their "Base64 Alphabet" equivalents as specified
         * in "Table 1: The Base64 Alphabet" of RFC 2045 (and RFC 4648).
         */
        private static final byte[] BASE_64_CHAR = {
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
        };

        static final Encoder RFC4648 = new Encoder();

        private Encoder() {
        }

        /**
         * Encodes all bytes from the specified byte array into a newly-allocated
         * byte array using the {@link Base64} encoding scheme. The returned byte
         * array is of the length of the resulting bytes.
         *
         * @param   src
         *          the byte array to encode
         * @return  A newly-allocated byte array containing the resulting
         *          encoded bytes.
         */
        public String encode(byte[] src) {
            int srcLen = src.length;
            byte[] dst = new byte[4 * ((srcLen + 2) / 3)];
            int fullDataLen = srcLen / 3 * 3;
            int dstP = 0;
            int srcP = 0;
            for (; srcP < fullDataLen; srcP += 3) {
                int bits = (src[srcP] & 0xff) << 16
                        | (src[srcP + 1] & 0xff) << 8
                        | (src[srcP + 2] & 0xff);
                dst[dstP++] = BASE_64_CHAR[(bits >>> 18) & 0x3f];
                dst[dstP++] = BASE_64_CHAR[(bits >>> 12) & 0x3f];
                dst[dstP++] = BASE_64_CHAR[(bits >>> 6)  & 0x3f];
                dst[dstP++] = BASE_64_CHAR[bits & 0x3f];
            }
            if (srcP < srcLen) {               // 1 or 2 leftover bytes
                int b0 = src[srcP++] & 0xff;
                dst[dstP++] = BASE_64_CHAR[b0 >> 2];
                if (srcP == srcLen) {
                    dst[dstP++] = BASE_64_CHAR[(b0 << 4) & 0x3f];
                    dst[dstP++] = '=';
                } else {
                    int b1 = src[srcP] & 0xff;
                    dst[dstP++] = BASE_64_CHAR[(b0 << 4) & 0x3f | (b1 >> 4)];
                    dst[dstP++] = BASE_64_CHAR[(b1 << 2) & 0x3f];
                }
                dst[dstP] = '=';
            }

            return new String(dst);
        }
    }
}

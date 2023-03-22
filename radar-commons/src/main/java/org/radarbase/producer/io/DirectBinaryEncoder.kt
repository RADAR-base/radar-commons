/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.radarbase.producer.io

import io.ktor.utils.io.*
import org.apache.avro.io.BinaryData
import java.io.IOException
import java.util.*

/**
 * An [Encoder] for Avro's binary encoding that does not buffer output.
 *
 *
 * This encoder does not buffer writes, and as a result is slower than
 * [BufferedBinaryEncoder]. However, it is lighter-weight and useful when
 * the buffering in BufferedBinaryEncoder is not desired and/or the Encoder is
 * very short lived.
 *
 *
 * To construct, use
 * [EncoderFactory.directBinaryEncoder]
 *
 *
 * DirectBinaryEncoder is not thread-safe
 *
 * @see BinaryEncoder
 *
 * @see EncoderFactory
 *
 * @see Encoder
 *
 * @see Decoder
 */
class DirectBinaryEncoder(
    var out: ByteWriteChannel,
) : BinaryEncoder() {
    // the buffer is used for writing floats, doubles, and large longs.
    private val buf = ByteArray(12)

    @Throws(IOException::class)
    override suspend fun flush() {
        out.flush()
    }

    override fun close() {
        out.close()
    }

    @Throws(IOException::class)
    override suspend fun writeBoolean(b: Boolean) {
        out.writeByte(if (b) 1 else 0)
    }

    /*
   * buffering is slower for ints that encode to just 1 or two bytes, and and
   * faster for large ones. (Sun JRE 1.6u22, x64 -server)
   */
    @Throws(IOException::class)
    override suspend fun writeInt(n: Int) {
        val `val` = n shl 1 xor (n shr 31)
        if (`val` and 0x7F.inv() == 0) {
            out.writeByte(`val`)
            return
        } else if (`val` and 0x3FFF.inv() == 0) {
            out.writeByte(0x80 or `val`)
            out.writeByte(`val` ushr 7)
            return
        }
        val len = BinaryData.encodeInt(n, buf, 0)
        out.writeFully(buf, 0, len)
    }

    /*
   * buffering is slower for writeLong when the number is small enough to fit in
   * an int. (Sun JRE 1.6u22, x64 -server)
   */
    @Throws(IOException::class)
    override suspend fun writeLong(n: Long) {
        val `val` = n shl 1 xor (n shr 63) // move sign to low-order bit
        if (`val` and 0x7FFFFFFFL.inv() == 0L) {
            var i = `val`.toInt()
            while (i and 0x7F.inv() != 0) {
                out.writeByte((0x80 or i and 0xFF).toByte().toInt())
                i = i ushr 7
            }
            out.writeByte(i.toByte().toInt())
            return
        }
        val len = BinaryData.encodeLong(n, buf, 0)
        out.writeFully(buf, 0, len)
    }

    @Throws(IOException::class)
    override suspend fun writeFloat(f: Float) {
        val len = BinaryData.encodeFloat(f, buf, 0)
        out.writeFully(buf, 0, len)
    }

    @Throws(IOException::class)
    override suspend fun writeDouble(d: Double) {
        val len = BinaryData.encodeDouble(d, buf, 0)
        out.writeFully(buf, 0, len)
    }

    @Throws(IOException::class)
    override suspend fun writeFixed(bytes: ByteArray, start: Int, len: Int) {
        out.writeFully(bytes, start, len)
    }

    @Throws(IOException::class)
    override suspend fun writeZero() {
        out.writeByte(0)
    }
}

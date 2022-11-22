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

import org.apache.avro.util.Utf8
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * An abstract [Encoder] for Avro's binary encoding.
 *
 *
 * To construct and configure instances, use [EncoderFactory]
 *
 * @see EncoderFactory
 *
 * @see BufferedBinaryEncoder
 *
 * @see DirectBinaryEncoder
 *
 * @see BlockingBinaryEncoder
 *
 * @see Encoder
 *
 * @see Decoder
 */
abstract class BinaryEncoder : Encoder {
    @Throws(IOException::class)
    override suspend fun writeNull() {
    }

    @Throws(IOException::class)
    override suspend fun writeString(utf8: Utf8) {
        this.writeBytes(utf8.bytes, 0, utf8.byteLength)
    }

    @Throws(IOException::class)
    override suspend fun writeString(str: String) {
        if (str.isEmpty()) {
            writeZero()
            return
        }
        val bytes = str.toByteArray(StandardCharsets.UTF_8)
        writeInt(bytes.size)
        writeFixed(bytes, 0, bytes.size)
    }

    @Throws(IOException::class)
    override suspend fun writeBytes(bytes: ByteBuffer) {
        val len = bytes.limit() - bytes.position()
        if (0 == len) {
            writeZero()
        } else {
            writeInt(len)
            writeFixed(bytes)
        }
    }

    @Throws(IOException::class)
    override suspend fun writeBytes(bytes: ByteArray, start: Int, len: Int) {
        if (0 == len) {
            writeZero()
            return
        }
        writeInt(len)
        this.writeFixed(bytes, start, len)
    }

    @Throws(IOException::class)
    override suspend fun writeEnum(e: Int) {
        writeInt(e)
    }

    @Throws(IOException::class)
    override suspend fun writeArrayStart() {
    }

    @Throws(IOException::class)
    override suspend fun setItemCount(itemCount: Long) {
        if (itemCount > 0) {
            writeLong(itemCount)
        }
    }

    @Throws(IOException::class)
    override suspend fun startItem() {
    }

    @Throws(IOException::class)
    override suspend fun writeArrayEnd() {
        writeZero()
    }

    @Throws(IOException::class)
    override suspend fun writeMapStart() {
    }

    @Throws(IOException::class)
    override suspend fun writeMapEnd() {
        writeZero()
    }

    @Throws(IOException::class)
    override suspend fun writeIndex(unionIndex: Int) {
        writeInt(unionIndex)
    }

    /** Write a zero byte to the underlying output.  */
    @Throws(IOException::class)
    protected abstract suspend fun writeZero()

    /**
     * Returns the number of bytes currently buffered by this encoder. If this
     * Encoder does not buffer, this will always return zero.
     *
     *
     * Call [.flush] to empty the buffer to the underlying output.
     */
    open val bytesBuffered: Int = 0
}

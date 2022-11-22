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
import java.io.Closeable
import java.io.IOException
import java.nio.ByteBuffer

/**
 * Low-level support for serializing Avro values.
 *
 *
 * This class has two types of methods. One type of methods support the writing
 * of leaf values (for example, [.writeLong] and [.writeString]).
 * These methods have analogs in [Decoder].
 *
 *
 * The other type of methods support the writing of maps and arrays. These
 * methods are [.writeArrayStart], [.startItem], and
 * [.writeArrayEnd] (and similar methods for maps). Some implementations
 * of [Encoder] handle the buffering required to break large maps and
 * arrays into blocks, which is necessary for applications that want to do
 * streaming. (See [.writeArrayStart] for details on these methods.)
 *
 *
 * [EncoderFactory] contains Encoder construction and configuration
 * facilities.
 *
 * @see EncoderFactory
 *
 * @see Decoder
 */
interface Encoder: Closeable {
    /**
     * "Writes" a null value. (Doesn't actually write anything, but advances the
     * state of the parser if this class is stateful.)
     *
     * @throws AvroTypeException If this is a stateful writer and a null is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeNull()

    /**
     * Write a boolean value.
     *
     * @throws AvroTypeException If this is a stateful writer and a boolean is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeBoolean(b: Boolean)

    /**
     * Writes a 32-bit integer.
     *
     * @throws AvroTypeException If this is a stateful writer and an integer is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeInt(n: Int)

    /**
     * Write a 64-bit integer.
     *
     * @throws AvroTypeException If this is a stateful writer and a long is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeLong(n: Long)

    /**
     * Write a float.
     *
     * @throws IOException
     * @throws AvroTypeException If this is a stateful writer and a float is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeFloat(f: Float)

    /**
     * Write a double.
     *
     * @throws AvroTypeException If this is a stateful writer and a double is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeDouble(d: Double)

    /**
     * Write a Unicode character string.
     *
     * @throws AvroTypeException If this is a stateful writer and a char-string is
     * not expected
     */
    @Throws(IOException::class)
    suspend fun writeString(utf8: Utf8)

    /**
     * Write a Unicode character string. The default implementation converts the
     * String to a [Utf8]. Some Encoder implementations
     * may want to do something different as a performance optimization.
     *
     * @throws AvroTypeException If this is a stateful writer and a char-string is
     * not expected
     */
    @Throws(IOException::class)
    suspend fun writeString(str: String) {
        writeString(Utf8(str))
    }

    /**
     * Write a Unicode character string. If the CharSequence is an
     * [Utf8] it writes this directly, otherwise the
     * CharSequence is converted to a String via toString() and written.
     *
     * @throws AvroTypeException If this is a stateful writer and a char-string is
     * not expected
     */
    @Throws(IOException::class)
    suspend fun writeString(charSequence: CharSequence) {
        if (charSequence is Utf8) writeString(charSequence) else writeString(charSequence.toString())
    }

    /**
     * Write a byte string.
     *
     * @throws AvroTypeException If this is a stateful writer and a byte-string is
     * not expected
     */
    @Throws(IOException::class)
    suspend fun writeBytes(bytes: ByteBuffer)

    /**
     * Write a byte string.
     *
     * @throws AvroTypeException If this is a stateful writer and a byte-string is
     * not expected
     */
    @Throws(IOException::class)
    suspend fun writeBytes(bytes: ByteArray, start: Int, len: Int)

    /**
     * Writes a byte string. Equivalent to
     * <tt>writeBytes(bytes, 0, bytes.length)</tt>
     *
     * @throws IOException
     * @throws AvroTypeException If this is a stateful writer and a byte-string is
     * not expected
     */
    @Throws(IOException::class)
    suspend fun writeBytes(bytes: ByteArray) {
        writeBytes(bytes, 0, bytes.size)
    }

    /**
     * Writes a fixed size binary object.
     *
     * @param bytes The contents to write
     * @param start The position within <tt>bytes</tt> where the contents start.
     * @param len   The number of bytes to write.
     * @throws AvroTypeException If this is a stateful writer and a byte-string is
     * not expected
     * @throws IOException
     */
    @Throws(IOException::class)
    suspend fun writeFixed(bytes: ByteArray, start: Int, len: Int)

    /**
     * A shorthand for <tt>writeFixed(bytes, 0, bytes.length)</tt>
     *
     * @param bytes
     */
    @Throws(IOException::class)
    suspend fun writeFixed(bytes: ByteArray) {
        writeFixed(bytes, 0, bytes.size)
    }

    /** Writes a fixed from a ByteBuffer.  */
    @Throws(IOException::class)
    suspend fun writeFixed(bytes: ByteBuffer) {
        val pos = bytes.position()
        val len = bytes.limit() - pos
        if (bytes.hasArray()) {
            writeFixed(bytes.array(), bytes.arrayOffset() + pos, len)
        } else {
            val b = ByteArray(len)
            bytes.duplicate()[b, 0, len]
            writeFixed(b, 0, len)
        }
    }

    /**
     * Writes an enumeration.
     *
     * @param e
     * @throws AvroTypeException If this is a stateful writer and an enumeration is
     * not expected or the <tt>e</tt> is out of range.
     * @throws IOException
     */
    @Throws(IOException::class)
    suspend fun writeEnum(e: Int)

    /**
     * Call this method to start writing an array.
     *
     * When starting to serialize an array, call [.writeArrayStart]. Then,
     * before writing any data for any item call [.setItemCount] followed by a
     * sequence of [.startItem] and the item itself. The number of
     * [.startItem] should match the number specified in
     * [.setItemCount]. When actually writing the data of the item, you can
     * call any [Encoder] method (e.g., [.writeLong]). When all items of
     * the array have been written, call [.writeArrayEnd].
     *
     * As an example, let's say you want to write an array of records, the record
     * consisting of an Long field and a Boolean field. Your code would look
     * something like this:
     *
     * <pre>
     * out.writeArrayStart();
     * out.setItemCount(list.size());
     * for (Record r : list) {
     * out.startItem();
     * out.writeLong(r.longField);
     * out.writeBoolean(r.boolField);
     * }
     * out.writeArrayEnd();
    </pre> *
     *
     * @throws AvroTypeException If this is a stateful writer and an array is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeArrayStart()

    /**
     * Call this method before writing a batch of items in an array or a map. Then
     * for each item, call [.startItem] followed by any of the other write
     * methods of [Encoder]. The number of calls to [.startItem] must
     * be equal to the count specified in [.setItemCount]. Once a batch is
     * completed you can start another batch with [.setItemCount].
     *
     * @param itemCount The number of [.startItem] calls to follow.
     * @throws IOException
     */
    @Throws(IOException::class)
    suspend fun setItemCount(itemCount: Long)

    /**
     * Start a new item of an array or map. See [.writeArrayStart] for usage
     * information.
     *
     * @throws AvroTypeException If called outside of an array or map context
     */
    @Throws(IOException::class)
    suspend fun startItem()

    /**
     * Call this method to finish writing an array. See [.writeArrayStart] for
     * usage information.
     *
     * @throws AvroTypeException If items written does not match count provided to
     * [.writeArrayStart]
     * @throws AvroTypeException If not currently inside an array
     */
    @Throws(IOException::class)
    suspend fun writeArrayEnd()

    /**
     * Call this to start a new map. See [.writeArrayStart] for details on
     * usage.
     *
     * As an example of usage, let's say you want to write a map of records, the
     * record consisting of an Long field and a Boolean field. Your code would look
     * something like this:
     *
     * <pre>
     * out.writeMapStart();
     * out.setItemCount(list.size());
     * for (Map.Entry<String></String>, Record> entry : map.entrySet()) {
     * out.startItem();
     * out.writeString(entry.getKey());
     * out.writeLong(entry.getValue().longField);
     * out.writeBoolean(entry.getValue().boolField);
     * }
     * out.writeMapEnd();
    </pre> *
     *
     * @throws AvroTypeException If this is a stateful writer and a map is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeMapStart()

    /**
     * Call this method to terminate the inner-most, currently-opened map. See
     * [.writeArrayStart] for more details.
     *
     * @throws AvroTypeException If items written does not match count provided to
     * [.writeMapStart]
     * @throws AvroTypeException If not currently inside a map
     */
    @Throws(IOException::class)
    suspend fun writeMapEnd()

    /**
     * Call this method to write the tag of a union.
     *
     * As an example of usage, let's say you want to write a union, whose second
     * branch is a record consisting of an Long field and a Boolean field. Your code
     * would look something like this:
     *
     * <pre>
     * out.writeIndex(1);
     * out.writeLong(record.longField);
     * out.writeBoolean(record.boolField);
    </pre> *
     *
     * @throws AvroTypeException If this is a stateful writer and a map is not
     * expected
     */
    @Throws(IOException::class)
    suspend fun writeIndex(unionIndex: Int)

    suspend fun flush()
}

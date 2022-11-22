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
package org.radarbase.data

import org.apache.avro.Schema
import java.io.IOException

/** Encode Avro values with a given encoder. The encoder may take into account the schema
 * that the schema registry has listed for a given topic.  */
interface AvroEncoder {
    /** Create a new writer. This method is thread-safe, but the class it returns is not.  */
    @Throws(IOException::class)
    fun <T: Any> writer(schema: Schema, clazz: Class<out T>): AvroWriter<T> =
        writer(schema, clazz, schema)
    @Throws(IOException::class)
    fun <T: Any> writer(schema: Schema, clazz: Class<out T>, readerSchema: Schema): AvroWriter<T>

    interface AvroWriter<T: Any> {
        /**
         * Encode an object. This method is not thread-safe. Call
         * [.setReaderSchema] before calling encode.
         * @param object object to encode
         * @return byte array with serialized object.
         */
        @Throws(IOException::class)
        fun encode(`object`: T): ByteArray
    }
}

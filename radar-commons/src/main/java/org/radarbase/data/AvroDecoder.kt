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

/** Decode Avro values with a given encoder.  */
interface AvroDecoder {
    /** Create a new reader. This method is thread-safe, but the class it returns is not.  */
    @Throws(IOException::class)
    fun <T> reader(schema: Schema, clazz: Class<out T>): AvroReader<T>
    interface AvroReader<T> {
        /**
         * Decode an object from bytes. This method is not thread-safe. Equivalent to calling
         * decode(object, 0).
         */
        @Throws(IOException::class)
        fun decode(`object`: ByteArray): T

        /**
         * Decode an object from bytes. This method is not thread-safe.
         * @param object bytes to decode from
         * @param offset start offset to decode from.
         */
        @Throws(IOException::class)
        fun decode(`object`: ByteArray, offset: Int): T
    }
}

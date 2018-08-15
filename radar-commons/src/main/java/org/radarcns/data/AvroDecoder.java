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

package org.radarcns.data;

import org.apache.avro.Schema;

import java.io.IOException;

/** Decode Avro values with a given encoder. */
public interface AvroDecoder {
    /** Create a new reader. This method is thread-safe, but the class it returns is not. */
    <T> AvroReader<T> reader(Schema schema, Class<? extends T> clazz) throws IOException;

    interface AvroReader<T> {
        /**
         * Decode an object from bytes. This method is not thread-safe. Equivalent to calling
         * decode(object, 0).
         */
        T decode(byte[] object) throws IOException;

        /**
         * Decode an object from bytes. This method is not thread-safe.
         * @param object bytes to decode from
         * @param start start offset to decode from.
         */
        T decode(byte[] object, int start) throws IOException;
    }
}

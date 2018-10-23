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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.radarcns.producer.rest.ParsedSchemaMetadata;

/** Encode Avro values with a given encoder. The encoder may take into account the schema
 * that the schema registry has listed for a given topic. */
public interface AvroEncoder {
    /** Create a new writer. This method is thread-safe, but the class it returns is not. */
    <T> AvroWriter<T> writer(Schema schema, Class<? extends T> clazz) throws IOException;

    interface AvroWriter<T> {
        /**
         * Encode an object. This method is not thread-safe. Call
         * {@link #setReaderSchema(ParsedSchemaMetadata)} before calling encode.
         * @param object object to encode
         * @return byte array with serialized object.
         */
        byte[] encode(T object) throws IOException;

        /**
         * Update the schema that the server is lists for the current topic.
         * @param readerSchema schema listed by the schema registry.
         * @throws SchemaValidationException if the server schema is incompatible with the writer
         *                                   schema.
         */
        void setReaderSchema(ParsedSchemaMetadata readerSchema) throws SchemaValidationException;

        /**
         * Get the schema that the server lists.
         * @return schema as set by setReaderSchema or null if not called yet.
         */
        ParsedSchemaMetadata getReaderSchema();
    }
}

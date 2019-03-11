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

package org.radarbase.data;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaValidationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.radarbase.producer.rest.ParsedSchemaMetadata;

/** Encodes a String as Avro. */
public class StringEncoder implements AvroEncoder, AvroEncoder.AvroWriter<String> {
    private static final ObjectWriter JSON_ENCODER = new ObjectMapper().writer();
    private ParsedSchemaMetadata readerSchema;

    @SuppressWarnings("unchecked")
    @Override
    public <T> AvroWriter<T> writer(Schema schema, Class<? extends T> clazz) {
        if (schema.getType() != Schema.Type.STRING || !clazz.equals(String.class)) {
            throw new IllegalArgumentException(
                    "Cannot encode String with a different type than STRING.");
        }

        return (AvroWriter<T>) this;
    }

    @Override
    public byte[] encode(String object) throws IOException {
        return JSON_ENCODER.writeValueAsBytes(object);
    }

    @Override
    public void setReaderSchema(ParsedSchemaMetadata readerSchema)
            throws SchemaValidationException {
        if (readerSchema.getSchema().getType() != Type.STRING) {
            throw new SchemaValidationException(
                    Schema.create(Type.STRING),
                    readerSchema.getSchema(),
                    new IllegalArgumentException("Cannot convert type to STRING"));
        }
        this.readerSchema = readerSchema;

    }

    @Override
    public ParsedSchemaMetadata getReaderSchema() {
        return readerSchema;
    }
}

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.radarbase.producer.rest.ParsedSchemaMetadata;
import org.radarbase.data.AvroEncoder.AvroWriter;

/**
 * Encodes Avro records to bytes.
 */
public class AvroRecordWriter<T> implements AvroWriter<T> {
    private final Encoder encoder;
    private final ByteArrayOutputStream out;
    private final DatumWriter<T> writer;
    private ParsedSchemaMetadata serverSchema;

    /**
     * Writer for a given encoder, schema and writer.
     * @param encoderFactory encoder factory to use.
     * @param schema schema to write records with.
     * @param writer data writer
     * @param binary true if the data should be serialized with binary Avro encoding, false if it
     *               should be with JSON encoding.
     * @throws IOException if an encoder cannot be constructed.
     */
    public AvroRecordWriter(EncoderFactory encoderFactory, Schema schema, DatumWriter<T> writer,
            boolean binary) throws IOException {
        this.writer = writer;
        out = new ByteArrayOutputStream();
        if (binary) {
            encoder = encoderFactory.binaryEncoder(out, null);
        } else {
            encoder = encoderFactory.jsonEncoder(schema, out);
        }
    }

    @Override
    public byte[] encode(T record) throws IOException {
        try {
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } finally {
            out.reset();
        }
    }

    @Override
    public void setReaderSchema(ParsedSchemaMetadata readerSchema) {
        this.serverSchema = readerSchema;
    }

    @Override
    public ParsedSchemaMetadata getReaderSchema() {
        return serverSchema;
    }
}

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

package org.radarcns.util.serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.radarcns.producer.SchemaRetriever;
import org.radarcns.producer.rest.ParsedSchemaMetadata;
import org.radarcns.util.Serialization;

/**
 * Serialize Kafka producer data with Avro.
 */
public class KafkaAvroSerializer extends AbstractKafkaAvroSerde<Object> {
    private final byte[] header = new byte[5];
    private BinaryEncoder encoder;

    public KafkaAvroSerializer() {
        super();
    }

    public KafkaAvroSerializer(SchemaRetriever retriever) {
        super(retriever);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        Schema initialSchema = SchemaRetriever.getSchema(data);

        try {
            ParsedSchemaMetadata schema = schemaRetriever
                    .getOrSetSchemaMetadata(topic, ofValue, initialSchema, -1);

            if (data instanceof byte[]) {
                byte[] byteData = (byte[])data;
                byte[] result = new byte[byteData.length + 5];
                storeHeader(result, schema);
                System.arraycopy(byteData, 0, result, 5, byteData.length);
                return result;
            } else {
                storeHeader(header, schema);
                try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                    out.write(header);

                    encoder = this.encoderFactory.directBinaryEncoder(out, encoder);
                    Object writer;
                    if (data instanceof SpecificRecord) {
                        writer = new SpecificDatumWriter(schema.getSchema());
                    } else {
                        writer = new GenericDatumWriter(schema.getSchema());
                    }

                    //noinspection unchecked
                    ((DatumWriter) writer).write(data, encoder);
                    encoder.flush();
                    return out.toByteArray();
                }
            }
        } catch (IOException ex) {
            throw new SerializationException("Cannot match data with schema registry", ex);
        }
    }

    private static void storeHeader(byte[] buffer, ParsedSchemaMetadata schema) {
        buffer[0] = 0;
        Serialization.intToBytes(schema.getId(), buffer, 1);
    }
}

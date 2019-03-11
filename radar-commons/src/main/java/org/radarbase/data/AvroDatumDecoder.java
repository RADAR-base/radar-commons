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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/** An AvroDecoder to decode known SpecificRecord classes. */
public class AvroDatumDecoder implements AvroDecoder {
    private final DecoderFactory decoderFactory;
    private final boolean binary;
    private final GenericData genericData;

    /**
     * Decoder for Avro data.
     * @param genericData instance of GenericData or SpecificData that should implement
     *                    {@link GenericData#createDatumReader(Schema)}.
     * @param binary true if the read data has Avro binary encoding, false if it has Avro JSON
     *               encoding.
     */
    public AvroDatumDecoder(GenericData genericData, boolean binary) {
        this.genericData = genericData;
        this.decoderFactory = DecoderFactory.get();
        this.binary = binary;
    }

    @Override
    public <T> AvroReader<T> reader(Schema schema, Class<? extends T> clazz) {
        @SuppressWarnings("unchecked")
        DatumReader<T> reader = genericData.createDatumReader(schema);
        return new AvroRecordReader<>(schema, reader);
    }

    private class AvroRecordReader<T> implements AvroReader<T> {
        private final DatumReader<T> reader;
        private final Schema schema;
        private Decoder decoder;

        private AvroRecordReader(Schema schema, DatumReader<T> reader) {
            this.reader = reader;
            this.schema = schema;
            this.decoder = null;
        }

        @Override
        public T decode(byte[] record) throws IOException {
            return decode(record, 0);
        }

        @Override
        public T decode(byte[] record, int offset) throws IOException {
            if (binary) {
                decoder = decoderFactory.binaryDecoder(record, offset, record.length - offset,
                        (BinaryDecoder) decoder);
            } else {
                decoder = decoderFactory.jsonDecoder(schema,
                        new ByteArrayInputStream(record, offset, record.length - offset));
            }
            return reader.read(null, decoder);
        }
    }
}

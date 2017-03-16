/*
 * Copyright 2017 Kings College London and The Hyve
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

/** An AvroDecoder to decode known SpecificRecord classes */
public class SpecificRecordDecoder implements AvroDecoder {
    private final DecoderFactory decoderFactory;
    private final boolean binary;

    public SpecificRecordDecoder(boolean binary) {
        this.decoderFactory = DecoderFactory.get();
        this.binary = binary;
    }

    @Override
    public <T> AvroReader<T> reader(Schema schema, Class<T> clazz) throws IOException {
        if (!SpecificRecord.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Can only create readers for SpecificRecords.");
        }
        return new AvroRecordReader<>(schema, new SpecificDatumReader<T>(schema));
    }

    class AvroRecordReader<T> implements AvroReader<T> {
        private final DatumReader<T> reader;
        private final Schema schema;
        private Decoder decoder;

        AvroRecordReader(Schema schema, DatumReader<T> reader) throws IOException {
            this.reader = reader;
            this.schema = schema;
            this.decoder = null;
        }

        public T decode(byte[] record) throws IOException {
            return decode(record, 0);
        }

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

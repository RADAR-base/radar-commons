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
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;

/** An AvroEncoder to encode known SpecificRecord classes. */
public class SpecificRecordEncoder implements AvroEncoder {
    private final EncoderFactory encoderFactory;
    private final boolean binary;

    /**
     * Create a SpecificRecordEncoder.
     * @param binary whether to use binary encoding or JSON.
     */
    public SpecificRecordEncoder(boolean binary) {
        this.encoderFactory = EncoderFactory.get();
        this.binary = binary;
    }

    @Override
    public <T> AvroWriter<T> writer(Schema schema, Class<? extends T> clazz) throws IOException {
        if (!SpecificRecord.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Can only newClient readers for SpecificRecords.");
        }
        return new AvroRecordWriter<>(encoderFactory, schema, new SpecificDatumWriter<T>(schema),
                binary);
    }
}

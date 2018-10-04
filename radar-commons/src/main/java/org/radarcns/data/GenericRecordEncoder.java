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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;

/** An AvroEncoder to encode GenericRecord classes. */
public class GenericRecordEncoder implements AvroEncoder {
    private final EncoderFactory encoderFactory;
    private final boolean binary;

    /**
     * Create a SpecificRecordEncoder.
     * @param binary whether to use binary encoding or JSON.
     */
    public GenericRecordEncoder(boolean binary) {
        this.encoderFactory = EncoderFactory.get();
        this.binary = binary;
    }

    @Override
    public <T> AvroWriter<T> writer(Schema schema, Class<? extends T> clazz) throws IOException {
        return new AvroRecordWriter<>(
                encoderFactory, schema, new GenericDatumWriter<T>(schema), binary);
    }
}

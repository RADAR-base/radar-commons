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

import org.apache.avro.Schema;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import java.io.IOException;

/** Encodes a String as Avro */
public class StringEncoder implements AvroEncoder, AvroEncoder.AvroWriter<String> {
    private static final ObjectWriter jsonEncoder = new ObjectMapper().writer();

    @Override
    public <T> AvroWriter<T> writer(Schema schema, Class<T> clazz) {
        if (schema.getType() != Schema.Type.STRING || !clazz.equals(String.class)) {
            throw new IllegalArgumentException("Cannot encode String with a different type than STRING.");
        }
        // noinspection unchecked
        return (AvroWriter<T>)this;
    }

    @Override
    public byte[] encode(String object) throws IOException {
        return jsonEncoder.writeValueAsBytes(object);
    }
}

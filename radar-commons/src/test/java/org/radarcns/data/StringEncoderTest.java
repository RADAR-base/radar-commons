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
import org.apache.avro.Schema.Type;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Created by nivethika on 24-2-17.
 */
public class StringEncoderTest {

    @Test
    public void encodeString() throws IOException {
        StringEncoder encoder = new StringEncoder();
        Schema schema = Schema.create(Type.STRING);

        AvroEncoder.AvroWriter<String> keyEncoder = encoder.writer(schema, String.class);


        byte[] key = keyEncoder.encode("{\"userId\":\"a\",\"sourceId\":\"b\"}");
        assertTrue( new String(key).contains("userId"));
        assertTrue( new String(key).contains("sourceId"));
    }

}

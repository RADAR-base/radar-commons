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

import org.junit.Test;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.topic.AvroTopic;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by nivethika on 24-2-17.
 */
public class SpecificRecordDecoderTest {

    @Test
    public void decodeJson() throws IOException {
        SpecificRecordDecoder decoder = new SpecificRecordDecoder(false);
        AvroTopic<ObservationKey, EmpaticaE4BloodVolumePulse> topic = new AvroTopic<>("keeeeys", ObservationKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(), ObservationKey.class, EmpaticaE4BloodVolumePulse.class);
        AvroDecoder.AvroReader<ObservationKey> keyDecoder = decoder.reader(topic.getKeySchema(), topic.getKeyClass());
        AvroDecoder.AvroReader<EmpaticaE4BloodVolumePulse> valueDecoder = decoder.reader(topic.getValueSchema(), topic.getValueClass());

        ObservationKey key = keyDecoder.decode("{\"projectId\":{\"string\":\"test\"},\"userId\":\"a\",\"sourceId\":\"b\"}".getBytes());
        assertEquals(key.get("projectId"), "test");
        assertEquals(key.get("userId"), "a");
        assertEquals(key.get("sourceId"), "b");

        EmpaticaE4BloodVolumePulse value = valueDecoder.decode("{\"time\":0.0,\"timeReceived\":0.0,\"bloodVolumePulse\":0.0}".getBytes());
        assertEquals(value.get("time"), 0.0d);
        assertEquals(value.get("timeReceived"), 0.0d);
        assertEquals(value.get("bloodVolumePulse"), 0.0f);
    }

    @Test
    public void decodeBinary() throws IOException {

        SpecificRecordDecoder decoder = new SpecificRecordDecoder(true);
        AvroTopic<ObservationKey, EmpaticaE4BloodVolumePulse> topic = new AvroTopic<>("keeeeys", ObservationKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(), ObservationKey.class, EmpaticaE4BloodVolumePulse.class);
        AvroDecoder.AvroReader<ObservationKey> keyDecoder = decoder.reader(topic.getKeySchema(), topic.getKeyClass());
        AvroDecoder.AvroReader<EmpaticaE4BloodVolumePulse> valueDecoder = decoder.reader(topic.getValueSchema(), topic.getValueClass());

        // note that positive numbers are multiplied by two in avro binary encoding, due to the
        // zig-zag encoding schema used.
        // See http://avro.apache.org/docs/1.8.1/spec.html#binary_encoding
        // union type index 1, length 4, char t, char e, char s, char t, length 1, char a, length 1, char b
        byte[] inputKey = {2, 8, 116, 101, 115, 116, 2, 97, 2, 98};
        ObservationKey key = keyDecoder.decode(inputKey);
        assertEquals(key.get("projectId"), "test");
        assertEquals(key.get("userId"), "a");
        assertEquals(key.get("sourceId"), "b");

        byte[] inputValue = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        EmpaticaE4BloodVolumePulse value = valueDecoder.decode(inputValue);
        assertEquals(value.get("time"), 0.0d);
        assertEquals(value.get("timeReceived"), 0.0d);
        assertEquals(value.get("bloodVolumePulse"), 0.0f);
    }
}

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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;
import org.radarcns.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.AvroTopic;

/**
 * Created by nivethika on 24-2-17.
 */
public class SpecificRecordDecorderTest {

    @Test
    public void decordJson() throws IOException {
        SpecificRecordDecoder decoder = new SpecificRecordDecoder(false);
        AvroTopic<MeasurementKey, EmpaticaE4BloodVolumePulse> topic = new AvroTopic<>("keeeeys", MeasurementKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(), MeasurementKey.class, EmpaticaE4BloodVolumePulse.class);
        AvroDecoder.AvroReader<MeasurementKey> keyDecoder = decoder.reader(topic.getKeySchema(), topic.getKeyClass());
        AvroDecoder.AvroReader<EmpaticaE4BloodVolumePulse> valueDecoder = decoder.reader(topic.getValueSchema(), topic.getValueClass());

        MeasurementKey key = keyDecoder.decode("{\"userId\":\"a\",\"sourceId\":\"b\"}".getBytes());
        assertEquals(key.get("userId"), "a");
        assertEquals(key.get("sourceId"), "b");

        EmpaticaE4BloodVolumePulse value = valueDecoder.decode("{\"time\":0.0,\"timeReceived\":0.0,\"bloodVolumePulse\":0.0}".getBytes());
        assertEquals(value.get("time"), 0.0d);
        assertEquals(value.get("timeReceived"), 0.0d);
        assertEquals(value.get("bloodVolumePulse"), 0.0f);
    }

    @Test
    public void decordBinary() throws IOException {

        SpecificRecordDecoder decoder = new SpecificRecordDecoder(true);
        AvroTopic<MeasurementKey, EmpaticaE4BloodVolumePulse> topic = new AvroTopic<>("keeeeys", MeasurementKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(), MeasurementKey.class, EmpaticaE4BloodVolumePulse.class);
        AvroDecoder.AvroReader<MeasurementKey> keyDecoder = decoder.reader(topic.getKeySchema(), topic.getKeyClass());
        AvroDecoder.AvroReader<EmpaticaE4BloodVolumePulse> valueDecoder = decoder.reader(topic.getValueSchema(), topic.getValueClass());

        byte[] inputKey = {2, 97, 2, 98};
        MeasurementKey key = keyDecoder.decode( inputKey);
        assertEquals(key.get("userId"), "a");
        assertEquals(key.get("sourceId"), "b");

        byte[] inputValue = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        EmpaticaE4BloodVolumePulse value = valueDecoder.decode(inputValue);
        assertEquals(value.get("time"), 0.0d);
        assertEquals(value.get("timeReceived"), 0.0d);
        assertEquals(value.get("bloodVolumePulse"), 0.0f);
    }
}

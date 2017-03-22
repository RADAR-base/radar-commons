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

import junit.framework.TestCase;

import org.radarcns.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.key.MeasurementKey;

import java.io.IOException;
import java.util.Arrays;
import org.radarcns.topic.AvroTopic;

public class SpecificRecordEncoderTest extends TestCase {
    public void testJson() throws IOException {
        SpecificRecordEncoder encoder = new SpecificRecordEncoder(false);
        AvroTopic<MeasurementKey, EmpaticaE4BloodVolumePulse> topic = new AvroTopic<>("keeeeys", MeasurementKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(), MeasurementKey.class, EmpaticaE4BloodVolumePulse.class);
        AvroEncoder.AvroWriter<MeasurementKey> keyEncoder = encoder.writer(topic.getKeySchema(), topic.getKeyClass());
        AvroEncoder.AvroWriter<EmpaticaE4BloodVolumePulse> valueEncoder = encoder.writer(topic.getValueSchema(), topic.getValueClass());

        byte[] key = keyEncoder.encode(new MeasurementKey("a", "b"));
        byte[] value = valueEncoder.encode(new EmpaticaE4BloodVolumePulse(0d, 0d, 0f));
        assertEquals("{\"userId\":\"a\",\"sourceId\":\"b\"}", new String(key));
        assertEquals("{\"time\":0.0,\"timeReceived\":0.0,\"bloodVolumePulse\":0.0}", new String(value));
    }

    public void testBinary() throws IOException {
        SpecificRecordEncoder encoder = new SpecificRecordEncoder(true);
        AvroTopic<MeasurementKey, EmpaticaE4BloodVolumePulse> topic = new AvroTopic<>("keeeeys", MeasurementKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(), MeasurementKey.class, EmpaticaE4BloodVolumePulse.class);
        AvroEncoder.AvroWriter<MeasurementKey> keyEncoder = encoder.writer(topic.getKeySchema(), topic.getKeyClass());
        AvroEncoder.AvroWriter<EmpaticaE4BloodVolumePulse> valueEncoder = encoder.writer(topic.getValueSchema(), topic.getValueClass());

        byte[] key = keyEncoder.encode(new MeasurementKey("a", "b"));
        // start string, char a, start string, char b
        byte[] expectedKey = {2, 97, 2, 98};
        System.out.println("key:      0x" + byteArrayToHex(key));
        System.out.println("expected: 0x" + byteArrayToHex(expectedKey));
        assertTrue(Arrays.equals(expectedKey, key));
        byte[] value = valueEncoder.encode(new EmpaticaE4BloodVolumePulse(0d, 0d, 0f));
        // 8 bytes, 8 bytes, 4 bytes, all zero
        byte[] expectedValue = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        System.out.println("value:    0x" + byteArrayToHex(value));
        System.out.println("expected: 0x" + byteArrayToHex(expectedValue));
        assertTrue(Arrays.equals(expectedValue, value));
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for(byte b: a)
            sb.append(String.format("%02x", b & 0xff));
        return sb.toString();
    }
}

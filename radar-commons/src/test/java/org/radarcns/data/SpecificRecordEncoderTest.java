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
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.TestCase;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.passive.phone.PhoneAcceleration;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpecificRecordEncoderTest extends TestCase {
    private static final Logger logger = LoggerFactory.getLogger(SpecificRecordEncoderTest.class);

    public void testJson() throws IOException {
        SpecificRecordEncoder encoder = new SpecificRecordEncoder(false);
        AvroTopic<ObservationKey, EmpaticaE4BloodVolumePulse> topic = new AvroTopic<>("keeeeys", ObservationKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(), ObservationKey.class, EmpaticaE4BloodVolumePulse.class);
        AvroEncoder.AvroWriter<ObservationKey> keyEncoder = encoder.writer(topic.getKeySchema(), topic.getKeyClass());
        AvroEncoder.AvroWriter<EmpaticaE4BloodVolumePulse> valueEncoder = encoder.writer(topic.getValueSchema(), topic.getValueClass());

        byte[] key = keyEncoder.encode(new ObservationKey("test", "a", "b"));
        byte[] value = valueEncoder.encode(new EmpaticaE4BloodVolumePulse(0d, 0d, 0f));
        assertEquals("{\"projectId\":{\"string\":\"test\"},\"userId\":\"a\",\"sourceId\":\"b\"}", new String(key));
        assertEquals("{\"time\":0.0,\"timeReceived\":0.0,\"bloodVolumePulse\":0.0}", new String(value));
    }

    public void testBinary() throws IOException {
        SpecificRecordEncoder encoder = new SpecificRecordEncoder(true);
        AvroTopic<ObservationKey, EmpaticaE4BloodVolumePulse> topic = new AvroTopic<>("keeeeys", ObservationKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(), ObservationKey.class, EmpaticaE4BloodVolumePulse.class);
        AvroEncoder.AvroWriter<ObservationKey> keyEncoder = encoder.writer(topic.getKeySchema(), topic.getKeyClass());
        AvroEncoder.AvroWriter<EmpaticaE4BloodVolumePulse> valueEncoder = encoder.writer(topic.getValueSchema(), topic.getValueClass());

        byte[] key = keyEncoder.encode(new ObservationKey("test", "a", "b"));
        // note that positive numbers are multiplied by two in avro binary encoding, due to the
        // zig-zag encoding schema used.
        // See http://avro.apache.org/docs/1.8.1/spec.html#binary_encoding
        // type index 1, length 4, char t, char e, char s, char t, length 1, char a, length 1, char b
        byte[] expectedKey = {2, 8, 116, 101, 115, 116, 2, 97, 2, 98};
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

    public void testSize() throws IOException {
        int n = 100;

        ThreadLocalRandom random = ThreadLocalRandom.current();
        AvroTopic<ObservationKey, PhoneAcceleration> topic = new AvroTopic<>("testie", ObservationKey.getClassSchema(), PhoneAcceleration.getClassSchema(), ObservationKey.class, PhoneAcceleration.class);
        ObservationKey key = new ObservationKey("my project", UUID.randomUUID().toString(), UUID.randomUUID().toString());
        double now = System.currentTimeMillis() / 1000d;

        SpecificRecordEncoder binEncoder = new SpecificRecordEncoder(true);
        AvroEncoder.AvroWriter<ObservationKey> binKeyEncoder = binEncoder.writer(topic.getKeySchema(), topic.getKeyClass());
        AvroEncoder.AvroWriter<PhoneAcceleration> binValueEncoder = binEncoder.writer(topic.getValueSchema(), topic.getValueClass());

        int binaryLength = n * binKeyEncoder.encode(key).length;
        for (int i = 0; i < 100; i++) {
            binaryLength += binValueEncoder.encode(new PhoneAcceleration(now, now, random.nextFloat(), random.nextFloat(), random.nextFloat())).length;
            now += 0.001;
        }

        SpecificRecordEncoder encoder = new SpecificRecordEncoder(false);
        AvroEncoder.AvroWriter<ObservationKey> keyEncoder = encoder.writer(topic.getKeySchema(), topic.getKeyClass());
        AvroEncoder.AvroWriter<PhoneAcceleration> valueEncoder = encoder.writer(topic.getValueSchema(), topic.getValueClass());

        int normalLength = n * (keyEncoder.encode(key).length + "{\"key\":".length());
        for (int i = 0; i < 100; i++) {
            normalLength += ",\"value\":},".length();
            normalLength += valueEncoder.encode(new PhoneAcceleration(now, now, random.nextFloat(), random.nextFloat(), random.nextFloat())).length;
            now += 0.001;
        }
        logger.info("Binary length: {}. Normal length: {}", binaryLength, normalLength);
        assertTrue(binaryLength < normalLength);
    }
}

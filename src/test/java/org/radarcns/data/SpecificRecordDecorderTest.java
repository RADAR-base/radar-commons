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

        MeasurementKey key = keyDecoder.decode( new String("{\"userId\":\"a\",\"sourceId\":\"b\"}").getBytes());
        assertEquals(key.get("userId"), "a");
        assertEquals(key.get("sourceId"), "b");

        EmpaticaE4BloodVolumePulse value = valueDecoder.decode(new String("{\"time\":0.0,\"timeReceived\":0.0,\"bloodVolumePulse\":0.0}").getBytes());
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

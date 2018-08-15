package org.radarcns.producer.rest;

import okio.Buffer;
import org.apache.avro.SchemaValidationException;
import org.junit.Test;
import org.radarcns.data.AvroRecordData;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.topic.AvroTopic;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;

public class BinaryRecordRequestTest {
    @Test
    public void writeToStream() throws SchemaValidationException, IOException {

        ObservationKey k = new ObservationKey("test", "a", "b");
        EmpaticaE4BloodVolumePulse v = new EmpaticaE4BloodVolumePulse(0.0, 0.0,
                0.0f);

        AvroTopic<ObservationKey, EmpaticaE4BloodVolumePulse> t = new AvroTopic<>(
                "t", k.getSchema(), v.getSchema(), k.getClass(), v.getClass());

        BinaryRecordRequest<ObservationKey, EmpaticaE4BloodVolumePulse> request = new BinaryRecordRequest<>(t);
        request.prepare(
                new ParsedSchemaMetadata(2, 1, k.getSchema()),
                new ParsedSchemaMetadata(4, 2, v.getSchema()),
                new AvroRecordData<>(t, k, Collections.singletonList(v)));

        // note that positive numbers are multiplied by two in avro binary encoding, due to the
        // zig-zag encoding schema used.
        // See http://avro.apache.org/docs/1.8.1/spec.html#binary_encoding
        byte[] expected = {
                2,  // key version x2
                4,  // value version x2
                2, (byte)'b',  // string length x2, sourceId
                2,  // number of records x2
                40,  // number of bytes in the first value x2
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // value
                0  // end of array
        };

        Buffer buffer = new Buffer();
        request.writeToSink(buffer);
        assertArrayEquals(expected, buffer.readByteArray());
    }
}

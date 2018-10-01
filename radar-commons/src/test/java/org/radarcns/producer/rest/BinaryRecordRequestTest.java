package org.radarcns.producer.rest;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import okio.Buffer;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.radarcns.data.AvroRecordData;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.kafka.RecordSet;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.topic.AvroTopic;

public class BinaryRecordRequestTest {

    // note that positive numbers are multiplied by two in avro binary encoding, due to the
    // zig-zag encoding schema used.
    // See http://avro.apache.org/docs/1.8.1/spec.html#binary_encoding
    private static final byte[] EXPECTED = {
        2,  // key version x2
        4,  // value version x2
        0,  // null project ID
        0,  // null user ID
        2, (byte)'b',  // string length x2, sourceId
        2,  // number of records x2
        40,  // number of bytes in the first value x2
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // value
        0  // end of array
    };

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


        Buffer buffer = new Buffer();
        request.writeToSink(buffer);
        assertArrayEquals(EXPECTED, buffer.readByteArray());
    }

    @Test
    public void expectedMatchesRecordSet() throws IOException {
        RecordSet recordSet = RecordSet.newBuilder()
            .setKeySchemaVersion(1)
            .setValueSchemaVersion(2)
            .setData(Collections.singletonList(ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})))
            .setProjectId(null)
            .setUserId(null)
            .setSourceId("b")
            .build();

        SpecificDatumWriter<RecordSet> writer = new SpecificDatumWriter<>(RecordSet.SCHEMA$);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(recordSet, encoder);
        encoder.flush();

        assertArrayEquals(EXPECTED, out.toByteArray());
    }
}

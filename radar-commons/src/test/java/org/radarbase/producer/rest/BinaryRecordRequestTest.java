package org.radarbase.producer.rest;

import static org.junit.Assert.assertArrayEquals;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import okio.Buffer;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.radarbase.data.AvroRecordData;
import org.radarbase.topic.AvroTopic;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.kafka.RecordSet;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.passive.phone.PhoneAcceleration;

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

    @Test
    public void testSize() throws IOException {


        SpecificDatumWriter<PhoneAcceleration> writer = new SpecificDatumWriter<>(PhoneAcceleration.SCHEMA$);

        List<ByteBuffer> records = new ArrayList<>(540);
        try (InputStream stream = BinaryRecordRequestTest.class.getResourceAsStream("android_phone_acceleration.csv");
                InputStreamReader reader = new InputStreamReader(stream);
                BufferedReader br = new BufferedReader(reader)) {

            String line = br.readLine();
            BinaryEncoder encoder = null;

            while (line != null) {
                String[] values = line.split(",");
                PhoneAcceleration acc = new PhoneAcceleration(Double.parseDouble(values[0]),
                        Double.parseDouble(values[1]), Float.parseFloat(values[2]),
                        Float.parseFloat(values[3]), Float.parseFloat(values[4]));
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                encoder = EncoderFactory.get().binaryEncoder(out, encoder);
                writer.write(acc, encoder);
                encoder.flush();
                records.add(ByteBuffer.wrap(out.toByteArray()));

                line = br.readLine();
            }
        }

        RecordSet recordSet = RecordSet.newBuilder()
                .setKeySchemaVersion(1)
                .setValueSchemaVersion(2)
                .setData(records)
                .setProjectId(null)
                .setUserId(null)
                .setSourceId("596740ca-5875-4c97-87ab-a08405f36aff")
                .build();

        SpecificDatumWriter<RecordSet> recordWriter = new SpecificDatumWriter<>(RecordSet.SCHEMA$);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        recordWriter.write(recordSet, encoder);
        encoder.flush();

        System.out.println("Size of record set with " + records.size() + " entries: " + out.size());

        ByteArrayOutputStream gzippedOut = new ByteArrayOutputStream();
        GZIPOutputStream gzipOut = new GZIPOutputStream(gzippedOut);
        gzipOut.write(out.size());
        gzipOut.close();

        System.out.println("Gzipped size of record set with " + records.size() + " entries: " + gzippedOut.size());
    }
}

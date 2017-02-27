package org.radarcns.data;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Test;

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

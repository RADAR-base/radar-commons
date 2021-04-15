package org.radarbase.mock.data;

import static org.junit.Assert.assertArrayEquals;

import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.junit.Test;

public class MockCsvParserTest {
    @Test
    public void parseValue() {
        Schema schema = new Schema.Parser().parse("{\"type\":\"bytes\"}");
        ByteBuffer buffer = (ByteBuffer) MockCsvParser.parseValue(schema, "ab");
        assertArrayEquals(new byte[] { 105 }, buffer.array());
    }
}

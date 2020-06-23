package org.radarbase.util;

import static org.junit.Assert.*;

import java.util.concurrent.ThreadLocalRandom;
import kotlin.text.Charsets;
import org.junit.Test;
import org.radarbase.util.Base64.Encoder;

public class Base64Test {
    @Test
    public void encoderTest() {
        Encoder encoder = Base64.getEncoder();
        java.util.Base64.Encoder javaEncoder = java.util.Base64.getEncoder();

        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 2_000; i++) {
            byte[] src = new byte[i];
            random.nextBytes(src);
            String actual = encoder.encode(src);
            String expected = new String(javaEncoder.encode(src), Charsets.UTF_8);
            assertEquals(expected, actual);
        }
    }
}

package org.radarcns.util;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by joris on 22/02/2017.
 */
public class SerializationTest {
    @Test
    public void bytesToLong() throws Exception {
        byte[] input = {0, 0, 0, 0, 0, 0, 0, 1};
        assertEquals(1L, Serialization.bytesToLong(input, 0));
    }

    @Test
    public void longToBytes() throws Exception {
        byte[] buffer = new byte[8];
        Serialization.longToBytes(1L, buffer, 0);
        assertArrayEquals(new byte[] {0, 0, 0, 0, 0, 0, 0, 1}, buffer);
    }

    @Test
    public void longToBytesAndBack() throws Exception {
        byte[] buffer = new byte[8];
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            long value = random.nextLong();
            Serialization.longToBytes(value, buffer, 0);
            assertEquals(value, Serialization.bytesToLong(buffer, 0));
        }
    }

    @Test
    public void longToBytesAndOffset() throws Exception {
        Random random = new Random();
        byte[] buffer = new byte[8 + 256];
        random.nextBytes(buffer);

        for (int i = 0; i < 10; i++) {
            int offset = random.nextInt(256);
            long value = random.nextLong();
            Serialization.longToBytes(value, buffer, offset);
            assertEquals(value, Serialization.bytesToLong(buffer, offset));
        }
    }

    @Test
    public void intToBytes() throws Exception {
        byte[] buffer = new byte[4];
        Serialization.intToBytes(1, buffer, 0);
        assertArrayEquals(new byte[] {0, 0, 0, 1}, buffer);
    }

    @Test
    public void bytesToInt() throws Exception {
        byte[] input = {0, 0, 0, 1};
        assertEquals(1, Serialization.bytesToInt(input, 0));
    }

    @Test
    public void intToBytesAndOffset() throws Exception {
        Random random = new Random();
        byte[] buffer = new byte[4 + 256];
        random.nextBytes(buffer);

        for (int i = 0; i < 10; i++) {
            int offset = random.nextInt(256);
            int value = random.nextInt();
            Serialization.intToBytes(value, buffer, offset);
            assertEquals(value, Serialization.bytesToInt(buffer, offset));
        }
    }
}
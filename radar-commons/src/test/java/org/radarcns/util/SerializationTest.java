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
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Serialization utility class. */
public final class Serialization {

    private Serialization() {
        // utility class
    }

    /** Read a little-endian encoded long from given bytes, starting from startIndex. */
    public static long bytesToLong(byte[] b, int startIndex) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= b[i + startIndex] & 0xFF;
        }
        return result;
    }

    /** Write a long to given bytes with little-endian encoding, starting from startIndex. */
    public static void longToBytes(long value, byte[] b, int startIndex) {
        b[startIndex] = (byte)((value >> 56) & 0xFF);
        b[startIndex + 1] = (byte)((value >> 48) & 0xFF);
        b[startIndex + 2] = (byte)((value >> 40) & 0xFF);
        b[startIndex + 3] = (byte)((value >> 32) & 0xFF);
        b[startIndex + 4] = (byte)((value >> 24) & 0xFF);
        b[startIndex + 5] = (byte)((value >> 16) & 0xFF);
        b[startIndex + 6] = (byte)((value >> 8) & 0xFF);
        b[startIndex + 7] = (byte)(value & 0xFF);
    }

    /** Write an int to given bytes with little-endian encoding, starting from startIndex. */
    public static void intToBytes(int value, byte[] b, int startIndex) {
        b[startIndex] = (byte)((value >> 24) & 0xFF);
        b[startIndex + 1] = (byte)((value >> 16) & 0xFF);
        b[startIndex + 2] = (byte)((value >> 8) & 0xFF);
        b[startIndex + 3] = (byte)(value & 0xFF);
    }

    /** Read a little-endian encoded int from given bytes, starting from startIndex. */
    public static int bytesToInt(byte[] b, int startIndex) {
        int result = 0;
        for (int i = 0; i < 4; i++) {
            result <<= 8;
            result |= b[i + startIndex] & 0xFF;
        }
        return result;
    }

    /** Read a little-endian encoded short from given bytes, starting from startIndex. */
    public static short bytesToShort(byte[] b, int startIndex) {
        short result = 0;
        for (int i = 0; i < 2; i++) {
            result <<= 8;
            result |= b[i + startIndex] & 0xFF;
        }
        return result;
    }

    /**
     * Convert a boolean to a byte.
     * @return -1 if b is null, 1 if b, and 0 if not b
     */
    public static byte booleanToByte(Boolean b) {
        if (b == null) {
            return -1;
        } else if (b.equals(Boolean.TRUE)) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * Read a boolean from a byte.
     * @return null if b == -1, true if b == 1, false otherwise.
     */
    public static Boolean byteToBoolean(byte b) {
        if (b == -1) {
            return null;
        } else if (b == 1) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    /**
     * Convert a float to a double using its apparent value. This avoids casting to the double
     * value closest to the mathematical value of a float.
     */
    public static double floatToDouble(float value) {
        return Double.parseDouble(Float.toString(value));
    }

    public static void copyStream(byte[] buffer, InputStream in, OutputStream out)
            throws IOException {
        int len = in.read(buffer);
        while (len != -1) {
            out.write(buffer, 0, len);
            len = in.read(buffer);
        }
    }
}

/*
 * Copyright 2017 Kings College London and The Hyve
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

public class Serialization {
    public static long bytesToLong(byte[] b, int startIndex) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= b[i + startIndex] & 0xFF;
        }
        return result;
    }

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

    public static void intToBytes(int value, byte[] b, int startIndex) {
        b[startIndex] = (byte)((value >> 24) & 0xFF);
        b[startIndex + 1] = (byte)((value >> 16) & 0xFF);
        b[startIndex + 2] = (byte)((value >> 8) & 0xFF);
        b[startIndex + 3] = (byte)(value & 0xFF);
    }

    public static int bytesToInt(byte[] b, int startIndex) {
        int result = 0;
        for (int i = 0; i < 4; i++) {
            result <<= 8;
            result |= b[i + startIndex] & 0xFF;
        }
        return result;
    }

    public static short bytesToShort(byte[] b, int startIndex) {
        short result = 0;
        for (int i = 0; i < 2; i++) {
            result <<= 8;
            result |= b[i + startIndex] & 0xFF;
        }
        return result;
    }

    public static byte booleanToByte(Boolean b) {
        if (b == null) {
            return -1;
        } else if (b.equals(Boolean.TRUE)) {
            return 1;
        } else {
            return 0;
        }
    }

    public static Boolean byteToBoolean(byte b) {
        if (b == -1) {
            return null;
        } else if (b == 1) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }
}

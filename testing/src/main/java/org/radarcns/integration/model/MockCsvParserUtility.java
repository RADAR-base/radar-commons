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

package org.radarcns.integration.model;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;

/**
 * A Utility class to support {@link MockConfigToCsvParser} operations
 */
public final class MockCsvParserUtility {

    private MockCsvParserUtility() {

    }

    /**
     * @param schema Avro schema @see {@link Schema}.
     * @param fieldString value that has to be deserialised.
     * @return the input value instantiated according to the type stated by the {@code Schema}.
     **/
    static Object parseValue(Schema schema, String fieldString) {
        switch (schema.getType()) {
            case INT:
                return Integer.parseInt(fieldString);
            case LONG:
                return Long.parseLong(fieldString);
            case FLOAT:
                return Float.parseFloat(fieldString);
            case DOUBLE:
                return Double.parseDouble(fieldString);
            case BOOLEAN:
                return Boolean.parseBoolean(fieldString);
            case STRING:
                return fieldString;
            case ARRAY:
                return parseArray(schema, fieldString);
            default:
                throw new IllegalArgumentException("Cannot handle schemas of type "
                    + schema.getType());
        }
    }

    /**
     * @param schema Avro schema. * @see {@link Schema}.
     * @param fieldString value that has to be deserialised.
     * @return the input value deserialised ad {@code List<Object>}.
     **/
    static List<Object> parseArray(Schema schema, String fieldString) {
        if (fieldString.charAt(0) != '['
                || fieldString.charAt(fieldString.length() - 1) != ']') {
            throw new IllegalArgumentException("Array must be enclosed by brackets.");
        }

        List<String> subStrings = new ArrayList<>();
        StringBuilder buffer = new StringBuilder(fieldString.length());
        int depth = 0;
        for (char c : fieldString.substring(1, fieldString.length() - 1).toCharArray()) {
            if (c == ';' && depth == 0) {
                subStrings.add(buffer.toString());
                buffer.setLength(0);
            } else {
                buffer.append(c);
                if (c == '[') {
                    depth++;
                } else if (c == ']') {
                    depth--;
                }
            }
        }
        if (buffer.length() > 0) {
            subStrings.add(buffer.toString());
        }

        List ret = new ArrayList(subStrings.size());
        for (String substring : subStrings) {
            ret.add(parseValue(schema.getElementType(), substring));
        }
        return ret;
    }

    /**
     * @param value String value that has to be deserialised.
     * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT representing the
     * initial time of a Kafka time window.
     **/
    static Long getStartTimeWindow(String value) {
        Double timeDouble = Double.parseDouble(value) / 10d;
        return timeDouble.longValue() * 10000;
    }

    /**
     * @param value String value that has to be deserialised.
     * @return the number of milliseconds since January 1, 1970, 00:00:00 GMT represented by this
     * String input.
     **/
    static Long getTimestamp(String value) {
        Double timeDouble = Double.valueOf(value) * 1000d;
        return timeDouble.longValue();
    }

}

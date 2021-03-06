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

package org.radarbase.mock.data;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.radarbase.data.Record;
import org.radarbase.mock.config.MockDataConfig;
import org.radarbase.topic.AvroTopic;

/**
 * Parse mock data from a CSV file
 * @param <K> key type.
 */
public class MockCsvParser<K extends SpecificRecord> implements Closeable {
    private static final char ARRAY_SEPARATOR = ';';
    private static final char ARRAY_START = '[';
    private static final char ARRAY_END = ']';

    private final AvroTopic<K, SpecificRecord> topic;
    private final Map<String, Integer> headerMap;
    private final CSVReader csvReader;
    private final BufferedReader bufferedReader;
    private String[] currentLine;

    /**
     * Base constructor.
     * @param config configuration of the stream.
     * @param root parent directory of the data file.
     * @throws IllegalArgumentException if the second row has the wrong number of columns
     */
    public MockCsvParser(MockDataConfig config, Path root)
            throws IOException, CsvValidationException {
        topic = config.parseAvroTopic();

        bufferedReader = Files.newBufferedReader(config.getDataFile(root));
        csvReader = new CSVReader(bufferedReader);
        String[] header = csvReader.readNext();
        headerMap = new HashMap<>();
        for (int i = 0; i < header.length; i++) {
            headerMap.put(header[i], i);
        }
        currentLine = csvReader.readNext();
    }

    public AvroTopic<K, SpecificRecord> getTopic() {
        return topic;
    }

    /**
     * Read the next record in the file.
     * @throws NullPointerException if a field from the Avro schema is missing as a column
     * @throws IllegalArgumentException if the row has the wrong number of columns
     * @throws IllegalStateException if a next row is not available
     * @throws IOException if the next row could not be read
     */
    public Record<K, SpecificRecord> next() throws IOException, CsvValidationException {
        if (!hasNext()) {
            throw new IllegalStateException("No next record available");
        }

        K key = parseRecord(currentLine, headerMap,
                topic.getKeyClass(), topic.getKeySchema());
        SpecificRecord value = parseRecord(currentLine, headerMap,
                topic.getValueClass(), topic.getValueSchema());

        currentLine = csvReader.readNext();

        return new Record<>(key, value);
    }

    /**
     * Whether there is a next record in the file.
     */
    public boolean hasNext() {
        return currentLine != null;
    }

    private <V extends SpecificRecord> V parseRecord(String[] rawValues,
            Map<String, Integer> header, Class<V> recordClass, Schema schema) {
        @SuppressWarnings("unchecked")
        V record = (V) SpecificData.newInstance(recordClass, schema);

        for (Field field : schema.getFields()) {
            Integer fieldHeader = header.get(field.name());
            if (fieldHeader == null) {
                throw new IllegalArgumentException(
                        "Cannot map record field " + field.name()
                                + ": no corresponding header in " + header.keySet());
            }
            String fieldString = rawValues[fieldHeader];
            Object fieldValue = parseValue(field.schema(), fieldString);
            record.put(field.pos(), fieldValue);
        }

        return record;
    }

    public static Object parseValue(Schema schema, String fieldString) {
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
            case UNION:
                return parseUnion(schema, fieldString);
            case ENUM:
                return parseEnum(schema, fieldString);
            case BYTES:
                return parseBytes(fieldString);
            default:
                throw new IllegalArgumentException("Cannot handle schemas of type "
                        + schema.getType());
        }
    }

    private static ByteBuffer parseBytes(String fieldString) {
        byte[] result = Base64.getDecoder()
                .decode(fieldString.getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(result);
    }

    private static Object parseUnion(Schema schema, String fieldString) {
        if (schema.getTypes().size() != 2) {
            throw new IllegalArgumentException(
                    "Cannot handle UNION types with other than two internal types: "
                    + schema.getTypes());
        }
        Schema schema0 = schema.getTypes().get(0);
        Schema schema1 = schema.getTypes().get(1);

        Schema nonNullSchema;
        if (schema0.getType() == Schema.Type.NULL) {
            nonNullSchema = schema1;
        } else if (schema1.getType() == Schema.Type.NULL) {
            nonNullSchema = schema0;
        } else {
            throw new IllegalArgumentException("Cannot handle non-nullable UNION types: "
                    + schema.getTypes());
        }

        if (fieldString.isEmpty() || fieldString.equals("null")) {
            return null;
        } else {
            return parseValue(nonNullSchema, fieldString);
        }
    }

    private static List<Object> parseArray(Schema schema, String fieldString) {
        if (fieldString.charAt(0) != ARRAY_START
                || fieldString.charAt(fieldString.length() - 1) != ARRAY_END) {
            throw new IllegalArgumentException("Array must be enclosed by brackets.");
        }

        List<String> subStrings = new ArrayList<>();
        StringBuilder buffer = new StringBuilder(fieldString.length());
        int depth = 0;
        for (char c : fieldString.substring(1, fieldString.length() - 1).toCharArray()) {
            if (c == ARRAY_SEPARATOR && depth == 0) {
                subStrings.add(buffer.toString());
                buffer.setLength(0);
            } else {
                buffer.append(c);
                if (c == ARRAY_START) {
                    depth++;
                } else if (c == ARRAY_END) {
                    depth--;
                }
            }
        }
        if (buffer.length() > 0) {
            subStrings.add(buffer.toString());
        }

        List<Object> ret = new ArrayList<>(subStrings.size());
        for (String substring : subStrings) {
            ret.add(parseValue(schema.getElementType(), substring));
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Enum<E>> E parseEnum(Schema schema, String fieldString) {
        try {
            Class<?> cls = Class.forName(schema.getFullName());
            Method valueOf = cls.getMethod("valueOf", String.class);
            return (E) valueOf.invoke(null, fieldString);
        } catch (ReflectiveOperationException | ClassCastException e) {
            throw new IllegalArgumentException(
                    "Cannot create enum class " + schema.getFullName()
                            + " for value " + fieldString, e);
        }
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
        bufferedReader.close();
    }

    @Override
    public String toString() {
        return "MockCsvParser{" + "topic=" + topic + '}';
    }
}

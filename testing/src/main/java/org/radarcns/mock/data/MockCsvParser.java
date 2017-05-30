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

package org.radarcns.mock.data;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.CsvParser;

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
    private final CsvParser csvReader;
    private final BufferedReader bufferedReader;
    private final FileReader fileReader;
    private List<String> currentLine;
    private long offset;

    /**
     * Base constructor.
     * @param config configuration of the stream.
     * @param root parent directory of the data file.
     * @throws IllegalArgumentException if the second row has the wrong number of columns
     */
    public MockCsvParser(MockDataConfig config, File root)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, IOException {
        //noinspection unchecked
        topic = (AvroTopic<K, SpecificRecord>) config.parseAvroTopic();

        fileReader = new FileReader(config.getDataFile(root));
        bufferedReader = new BufferedReader(fileReader);
        csvReader = new CsvParser(bufferedReader);
        List<String> header = csvReader.parseLine();
        headerMap = new HashMap<>();
        for (int i = 0; i < header.size(); i++) {
            headerMap.put(header.get(i), i);
        }
        currentLine = csvReader.parseLine();
        offset = 0;
    }

    public AvroTopic getTopic() {
        return topic;
    }

    /**
     * Read the next record in the file.
     * @throws NullPointerException if a field from the Avro schema is missing as a column
     * @throws IllegalArgumentException if the row has the wrong number of columns
     * @throws IllegalStateException if a next row is not available
     * @throws IOException if the next row could not be read
     */
    public Record<K, SpecificRecord> next() throws IOException {
        if (!hasNext()) {
            throw new IllegalStateException("No next record available");
        }

        K key = parseRecord(currentLine, headerMap,
                topic.getKeyClass(), topic.getKeySchema());
        SpecificRecord value = parseRecord(currentLine, headerMap,
                topic.getValueClass(), topic.getValueSchema());

        currentLine = csvReader.parseLine();

        return new Record<>(offset++, key, value);
    }

    /**
     * Whether there is a next record in the file.
     */
    public boolean hasNext() {
        return currentLine != null;
    }

    private <V extends SpecificRecord> V parseRecord(List<String> rawValues,
            Map<String, Integer> header, Class<V> recordClass, Schema schema) {
        @SuppressWarnings("unchecked")
        V record = (V) SpecificData.newInstance(recordClass, schema);

        for (Field field : schema.getFields()) {
            String fieldString = rawValues.get(header.get(field.name()));
            Object fieldValue = parseValue(field.schema(), fieldString);
            record.put(field.pos(), fieldValue);
        }

        return record;
    }

    private static Object parseValue(Schema schema, String fieldString) {
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

    @Override
    public void close() throws IOException {
        csvReader.close();
        bufferedReader.close();
        fileReader.close();
    }
}

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

package org.radarcns.mock;

import java.io.BufferedReader;
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
import org.radarcns.topic.AvroTopic;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.key.MeasurementKey;
import org.radarcns.util.CsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send mock data from a CSV file.
 *
 * <p>The value type is dynamic, so we will not check any of the generics.
 */
@SuppressWarnings("unchecked")
public class MockFile {
    private static final Logger logger = LoggerFactory.getLogger(MockFile.class);
    private static final char ARRAY_SEPARATOR = ';';
    private static final char ARRAY_START = '[';
    private static final char ARRAY_END = ']';

    private final MockDataConfig config;
    private final File baseFile;
    private final KafkaSender sender;
    private final AvroTopic topic;

    public MockFile(KafkaSender<MeasurementKey, SpecificRecord> sender, File baseFile,
            MockDataConfig config)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, IOException {
        this.baseFile = baseFile;
        this.config = config;
        this.sender = sender;

        Class<?> keyClass = Class.forName(this.config.getKeySchema());
        Schema keySchema = (Schema) keyClass.getMethod("getClassSchema").invoke(null);
        // check instantiation
        SpecificData.newInstance(keyClass, keySchema);

        Class<?> valueClass = Class.forName(this.config.getValueSchema());
        Schema valueSchema = (Schema) valueClass.getMethod("getClassSchema").invoke(null);
        // check instantiation
        SpecificData.newInstance(valueClass, valueSchema);

        topic = new AvroTopic(config.getTopic(), MeasurementKey.getClassSchema(), valueSchema,
                MeasurementKey.class, valueClass);

    }

    /**
     * Send data from the configured CSV file synchronously.
     * @throws IOException if data could not be read or sent.
     */
    public void send() throws IOException {
        File csvFile = config.getDataFile(baseFile);

        long offset = 0L;

        try (KafkaTopicSender topicSender = sender.sender(topic);
                FileReader fileReader = new FileReader(csvFile);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                CsvParser csvReader = new CsvParser(bufferedReader)) {

            List<String> header = csvReader.parseLine();
            Map<String, Integer> headerMap = new HashMap<>();
            for (int i = 0; i < header.size(); i++) {
                headerMap.put(header.get(i), i);
            }

            List<String> rawValues = csvReader.parseLine();
            while (rawValues != null) {
                SpecificRecord key = parseRecord(rawValues, headerMap,
                        topic.getKeyClass(), topic.getKeySchema());
                SpecificRecord value = parseRecord(rawValues, headerMap,
                        topic.getValueClass(), topic.getValueSchema());

                topicSender.send(offset, key, value);
                logger.info("Sent key {} and value {}", key, value);

                rawValues = csvReader.parseLine();
                offset++;
            }
        }
    }

    private SpecificRecord parseRecord(List<String> rawValues, Map<String, Integer> header,
            Class<?> recordClass, Schema schema) {
        SpecificRecord record = (SpecificRecord) SpecificData.newInstance(recordClass, schema);

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

        List ret = new ArrayList(subStrings.size());
        for (String substring : subStrings) {
            ret.add(parseValue(schema.getElementType(), substring));
        }
        return ret;
    }
}

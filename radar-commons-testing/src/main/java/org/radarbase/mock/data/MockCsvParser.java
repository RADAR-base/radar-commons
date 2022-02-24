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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificRecord;
import org.radarbase.data.Record;
import org.radarbase.mock.config.MockDataConfig;
import org.radarbase.producer.rest.SchemaRetriever;
import org.radarbase.topic.AvroTopic;

/**
 * Parse mock data from a CSV file.
 */
@SuppressWarnings("PMD.GodClass")
public class MockCsvParser implements Closeable {
    private final AvroTopic<GenericRecord, GenericRecord> topic;
    private final CSVReader csvReader;
    private final BufferedReader bufferedReader;
    private final Instant startTime;
    private final Duration rowDuration;
    private final HeaderHierarchy headers;
    private String[] currentLine;
    private int row;
    private long rowTime;

    /**
     * Base constructor.
     * @param config configuration of the stream.
     * @param root parent directory of the data file.
     * @param retriever schema retriever to fetch schema with if none is supplied.
     * @throws IllegalArgumentException if the second row has the wrong number of columns
     */
    public MockCsvParser(MockDataConfig config, Path root, Instant startTime,
            SchemaRetriever retriever)
            throws IOException, CsvValidationException {
        Schema keySchema, valueSchema;
        try {
            AvroTopic<SpecificRecord, SpecificRecord> specificTopic = config.parseAvroTopic();
            keySchema = specificTopic.getKeySchema();
            valueSchema = specificTopic.getValueSchema();
        } catch (IllegalStateException ex) {
            Objects.requireNonNull(retriever, "Cannot instantiate value schema without "
                    + "schema retriever.");
            keySchema = AvroTopic.parseSpecificRecord(config.getKeySchema()).getSchema();
            valueSchema = retriever.getBySubjectAndVersion(
                    config.getTopic(), true, 0).getSchema();
        }
        topic = new AvroTopic<>(config.getTopic(),
                keySchema, valueSchema,
                GenericRecord.class, GenericRecord.class);

        this.startTime = startTime;
        row = 0;
        rowDuration = Duration.ofMillis((long)(1.0 / config.getFrequency()));
        rowTime = this.startTime.toEpochMilli();

        bufferedReader = Files.newBufferedReader(config.getDataFile(root));
        csvReader = new CSVReader(bufferedReader);
        headers = new HeaderHierarchy();
        String[] header = csvReader.readNext();
        for (int i = 0; i < header.length; i++) {
            headers.add(i, List.of(header[i].split("\\.")));
        }
        currentLine = csvReader.readNext();
    }

    public AvroTopic<GenericRecord, GenericRecord> getTopic() {
        return topic;
    }

    /**
     * Read the next record in the file.
     * @throws NullPointerException if a field from the Avro schema is missing as a column
     * @throws IllegalArgumentException if the row has the wrong number of columns
     * @throws IllegalStateException if a next row is not available
     * @throws IOException if the next row could not be read
     */
    public Record<GenericRecord, GenericRecord> next() throws IOException, CsvValidationException {
        if (!hasNext()) {
            throw new IllegalStateException("No next record available");
        }

        GenericRecord key = parseRecord(currentLine, topic.getKeySchema(), headers.getChildren().get("key"));
        GenericRecord value = parseRecord(currentLine, topic.getValueSchema(), headers.getChildren().get("value"));

        currentLine = csvReader.readNext();
        row++;
        rowTime = startTime
                .plus(rowDuration.multipliedBy(row))
                .toEpochMilli();
        return new Record<>(key, value);
    }

    /**
     * Whether there is a next record in the file.
     */
    public boolean hasNext() {
        return currentLine != null;
    }

    private GenericRecord parseRecord(String[] rawValues, Schema schema, HeaderHierarchy headers) {
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        Map<String, HeaderHierarchy> children = headers.getChildren();

        for (Field field : schema.getFields()) {
            HeaderHierarchy child = children.get(field.name());
            if (child != null) {
                record.set(field, parseValue(rawValues, field.schema(), child));
            }
        }

        return record.build();
    }

    /** Parse value from Schema. */
    public Object parseValue(String[] rawValues, Schema schema, HeaderHierarchy headers) {
        switch (schema.getType()) {
            case NULL:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case ENUM:
            case BYTES:
                return parseScalar(rawValues, schema, headers);
            case UNION:
                return parseUnion(rawValues, schema, headers);
            case RECORD:
                return parseRecord(rawValues, schema, headers);
            case ARRAY:
                return parseArray(rawValues, schema, headers);
            case MAP:
                return parseMap(rawValues, schema, headers);
            default:
                throw new IllegalArgumentException("Cannot handle schemas of type "
                        + schema.getType() + " in " + headers);
        }
    }

    private Object parseScalar(String[] rawValues, Schema schema, HeaderHierarchy headers) {
        int fieldHeader = headers.getIndex();
        String fieldString = rawValues[fieldHeader]
                .replace("${timeSeconds}", Double.toString(rowTime / 1000.0))
                .replace("${timeMillis}", Long.toString(rowTime));

        return parseScalar(fieldString, schema, headers);
    }

    private static Object parseScalar(String fieldString, Schema schema, HeaderHierarchy headers) {
        switch (schema.getType()) {
            case NULL:
                if (fieldString == null || fieldString.isEmpty() || fieldString.equals("null")) {
                    return null;
                } else {
                    throw new IllegalArgumentException("Cannot parse " + fieldString + " as null");
                }
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
            case ENUM:
                return parseEnum(schema, fieldString);
            case BYTES:
                return parseBytes(fieldString);
            default:
                throw new IllegalArgumentException("Cannot handle scalar schema of type "
                        + schema.getType() + " in " + headers);
        }
    }

    private Map<String, Object> parseMap(String[] rawValues, Schema schema, HeaderHierarchy headers) {
        Map<String, HeaderHierarchy> children = headers.getChildren();
        Map<String, Object> map = new LinkedHashMap<>(children.size() * 4 / 3);
        for (HeaderHierarchy child : children.values()) {
            map.put(child.getName(), parseValue(rawValues, schema.getValueType(), child));
        }
        return map;
    }

    private static ByteBuffer parseBytes(String fieldString) {
        byte[] result = Base64.getDecoder()
                .decode(fieldString.getBytes(StandardCharsets.UTF_8));
        return ByteBuffer.wrap(result);
    }

    private Object parseUnion(String[] rawValues, Schema schema, HeaderHierarchy headers) {
        for (Schema subschema : schema.getTypes()) {
            try {
                return parseValue(rawValues, subschema, headers);
            } catch (IllegalArgumentException ex) {
                // skip bad union member
            }
        }
        throw new IllegalArgumentException("Cannot handle union types "
                + schema.getTypes() + " in " + headers);
    }

    private List<Object> parseArray(String[] rawValues, Schema schema, HeaderHierarchy headers) {
        Map<String, HeaderHierarchy> children = headers.getChildren();
        int arrayLength = children.keySet().stream()
                .mapToInt(headerName -> Integer.parseInt(headerName) + 1)
                .max()
                .orElse(0);

        GenericData.Array<Object> array = new GenericData.Array<>(arrayLength, schema);
        for (int i = 0; i < arrayLength; i++) {
            HeaderHierarchy child = children.get(String.valueOf(i));
            if (child != null) {
                array.add(i, parseValue(rawValues, schema.getElementType(), child));
            } else {
                array.add(i, null);
            }
        }
        return array;
    }

    private static GenericData.EnumSymbol parseEnum(Schema schema, String fieldString) {
        return new GenericData.EnumSymbol(schema, fieldString);
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

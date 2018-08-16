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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.Metronome;

/**
 * Generates records according to the specification in a {@link MockDataConfig}.
 *
 * @param <K> type of key to generate
 */
public class RecordGenerator<K extends SpecificRecord> {
    private static final Set<Type> ACCEPTABLE_VALUE_TYPES = new HashSet<>(Arrays.asList(Type.DOUBLE,
            Type.FLOAT, Type.INT, Type.LONG));
    private final AvroTopic<K, SpecificRecord> topic;
    private final Field timeField;
    private final Field timeReceivedField;
    private final List<Field> valueFields;
    private final MockDataConfig config;
    private final List<Field> unknownFields;
    private final List<String> header;

    /**
     * Generates records according to config. Given key class must match the one specified in the
     * server.
     * @param config configuration to use
     */
    public RecordGenerator(MockDataConfig config, Class<K> keyClass) {
        this.config = config;

        // doing type checking below.
        topic = config.parseAvroTopic();
        if (!topic.getKeyClass().equals(keyClass)) {
            throw new IllegalArgumentException(
                    "RecordGenerator only generates ObservationKey keys, not "
                            + topic.getKeyClass() + " in topic " + topic);
        }
        if (!SpecificRecord.class.isAssignableFrom(topic.getValueClass())) {
            throw new IllegalArgumentException(
                    "RecordGenerator only generates SpecificRecord values, not "
                            + topic.getValueClass() + " in topic " + topic);
        }
        header = new ArrayList<>();
        header.addAll(Arrays.asList("projectId", "userId", "sourceId"));

        // cache key and value fields
        Schema valueSchema = topic.getValueSchema();

        timeField = forceGetField(valueSchema, "time");
        timeReceivedField = forceGetField(valueSchema, "timeReceived");

        List<String> valueFieldNames = config.getValueFields();
        if (valueFieldNames == null) {
            valueFieldNames = Collections.emptyList();
        }
        valueFields = new ArrayList<>(valueFieldNames.size());
        for (String fieldName : valueFieldNames) {
            Field field = forceGetField(valueSchema, fieldName);
            valueFields.add(field);
            Schema.Type type = field.schema().getType();
            if (!ACCEPTABLE_VALUE_TYPES.contains(type)) {
                throw new IllegalArgumentException("Cannot generate data for type " + type
                        + " in field " + fieldName + " in topic " + topic);
            }
        }

        unknownFields = new ArrayList<>(valueSchema.getFields().size() - valueFields.size() - 2);
        for (Field field : valueSchema.getFields()) {
            header.add(field.name());
            if (field.name().equals("time") || field.name().equals("timeReceived")
                    || valueFieldNames.contains(field.name())) {
                continue;
            }
            unknownFields.add(field);
        }
    }

    public List<String> getHeader() {
        return header;
    }

    /** Get given schema field, and throw an IllegalArgumentException if it does not exists. */
    private Field forceGetField(Schema schema, String name) {
        Field field = schema.getField(name);
        if (field == null) {
            throw new IllegalArgumentException("Schema for topic " + topic + " does not contain "
                    + "required field " + name);
        }
        return field;
    }

    /**
     * Simulates data of a sensor with the given frequency for a time interval specified by
     *      duration. The data is converted to lists of strings.
     * @param duration in milliseconds for the simulation
     * @param key key to generate data with
     * @return list containing simulated values
     */
    public Iterator<List<String>> iterateRawValues(K key, long duration) {
        final Iterator<Record<K, SpecificRecord>> baseIterator = iterateValues(key,
                duration);
        return new RecordListIterator<>(baseIterator);
    }

    /**
     * Simulates data of a sensor with the given frequency for a time interval specified by
     *      duration.
     * @param duration in milliseconds for the simulation
     * @param key key to generate data with
     * @return list containing simulated values
     */
    public Iterator<Record<K, SpecificRecord>> iterateValues(final K key, final long duration) {
        return new RecordIterator(duration, key);
    }

    /**
     * @return random {@code Double} using {@code ThreadLocalRandom}.
     **/
    private double getRandomDouble() {
        return ThreadLocalRandom.current().nextDouble(config.getMinimum(), config.getMaximum());
    }

    /**
     * It returns the time a message is received.
     * @param time time at which the message has been sent
     * @return random {@code Double} representing the Round Trip Time for the given timestamp
     *      using {@code ThreadLocalRandom}
     **/
    private long getTimeReceived(long time) {
        return time + ThreadLocalRandom.current().nextLong(1 , 10);
    }

    private static class RecordListIterator<K extends SpecificRecord>
            implements Iterator<List<String>> {

        private final Iterator<Record<K, SpecificRecord>> baseIterator;

        private RecordListIterator(Iterator<Record<K, SpecificRecord>> baseIterator) {
            this.baseIterator = baseIterator;
        }

        @Override
        public boolean hasNext() {
            return baseIterator.hasNext();
        }

        @Override
        public List<String> next() {
            Record<K, SpecificRecord> record = baseIterator.next();

            int keyFieldsSize = record.key.getSchema().getFields().size();
            int valueFieldsSize = record.value.getSchema().getFields().size();
            List<String> result = new ArrayList<>(keyFieldsSize + valueFieldsSize);

            for (int i = 0; i < keyFieldsSize; i++) {
                result.add(record.key.get(i).toString());
            }
            for (int i = 0; i < valueFieldsSize; i++) {
                result.add(record.value.get(i).toString());
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }

    public MockDataConfig getConfig() {
        return config;
    }

    public AvroTopic<K, SpecificRecord> getTopic() {
        return topic;
    }

    private class RecordIterator implements Iterator<Record<K, SpecificRecord>> {
        private final Metronome timestamps;
        private final K key;

        public RecordIterator(long duration, K key) {
            this.key = key;
            timestamps = new Metronome(duration * config.getFrequency() / 1000L,
                    config.getFrequency());
        }

        @Override
        public boolean hasNext() {
            return timestamps.hasNext();
        }

        @Override
        public Record<K, SpecificRecord> next() {
            if (!hasNext()) {
                throw new IllegalStateException("Iterator done");
            }
            SpecificRecord value = topic.newValueInstance();
            long time = timestamps.next();

            value.put(timeField.pos(), time / 1000d);
            value.put(timeReceivedField.pos(), getTimeReceived(time) / 1000d);

            for (Field f : valueFields) {
                Type type = f.schema().getType();
                Object fieldValue;
                switch (type) {
                    case DOUBLE:
                        fieldValue = getRandomDouble();
                        break;
                    case FLOAT:
                        fieldValue = (float)getRandomDouble();
                        break;
                    case LONG:
                        fieldValue = (long)getRandomDouble();
                        break;
                    case INT:
                        fieldValue = (int)getRandomDouble();
                        break;
                    default:
                        throw new IllegalStateException("Cannot parse type " + type);

                }
                value.put(f.pos(), fieldValue);
            }

            for (Field f : unknownFields) {
                value.put(f.pos(), f.defaultVal());
            }

            return new Record<>(key, value);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

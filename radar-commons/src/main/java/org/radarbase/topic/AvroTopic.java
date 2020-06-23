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

package org.radarbase.topic;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

/** Kafka topic with schema. */
public class AvroTopic<K, V> extends KafkaTopic {
    private final Schema valueSchema;
    private final Schema keySchema;
    private final Schema.Type[] valueFieldTypes;
    private final Class<? extends V> valueClass;
    private final Class<? extends K> keyClass;

    /**
     * Kafka topic with Avro schema.
     * @param name topic name
     * @param keySchema Avro schema for keys
     * @param valueSchema Avro schema for values
     * @param keyClass Java class for keys
     * @param valueClass Java class for values
     */
    public AvroTopic(String name,
            Schema keySchema, Schema valueSchema,
            Class<? extends K> keyClass, Class<? extends V> valueClass) {
        super(name);

        if (keySchema == null || valueSchema == null || keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Topic values may not be null");
        }

        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.valueClass = valueClass;
        this.keyClass = keyClass;

        if (valueSchema.getType() == Type.RECORD) {
            List<Schema.Field> fields = valueSchema.getFields();
            this.valueFieldTypes = new Schema.Type[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                valueFieldTypes[i] = fields.get(i).schema().getType();
            }
        } else {
            this.valueFieldTypes = null;
        }
    }

    /** Avro schema used for keys. */
    public Schema getKeySchema() {
        return keySchema;
    }

    /** Avro schema used for values. */
    public Schema getValueSchema() {
        return valueSchema;
    }

    /** Java class used for keys. */
    public Class<? extends K> getKeyClass() {
        return keyClass;
    }

    /** Java class used for values. */
    public Class<? extends V> getValueClass() {
        return valueClass;
    }

    /**
     * Tries to construct a new SpecificData instance of the value.
     * @return new empty SpecificData class
     * @throws ClassCastException Value class is not a SpecificData class
     */
    @SuppressWarnings("unchecked")
    public V newValueInstance() throws ClassCastException {
        return (V)SpecificData.newInstance(valueClass, valueSchema);
    }

    public Schema.Type[] getValueFieldTypes() {
        return valueFieldTypes;
    }

    /**
     * Parse an AvroTopic.
     *
     * @throws IllegalArgumentException if the key_schema or value_schema properties are not valid
     *                                  Avro SpecificRecord classes
     */
    @SuppressWarnings({"unchecked", "JavaReflectionMemberAccess"})
    public static <K extends SpecificRecord, V extends SpecificRecord> AvroTopic<K, V> parse(
            String topic, String keySchema, String valueSchema) {

        try {
            Objects.requireNonNull(topic, "topic needs to be specified");
            Objects.requireNonNull(keySchema, "key_schema needs to be specified");
            Objects.requireNonNull(valueSchema, "value_schema needs to be specified");

            Class<? extends K> keyClass = (Class<? extends K>) Class.forName(keySchema);
            Schema keyAvroSchema = (Schema) keyClass
                    .getMethod("getClassSchema").invoke(null);
            // check instantiation
            SpecificData.newInstance(keyClass, keyAvroSchema);

            Class<? extends V> valueClass = (Class<? extends V>) Class.forName(valueSchema);
            Schema valueAvroSchema = (Schema) valueClass
                    .getMethod("getClassSchema").invoke(null);
            // check instantiation
            SpecificData.newInstance(valueClass, valueAvroSchema);

            return new AvroTopic<>(topic, keyAvroSchema, valueAvroSchema, keyClass, valueClass);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException
                | IllegalAccessException ex) {
            throw new IllegalArgumentException("Topic " + topic
                    + " schema cannot be instantiated", ex);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }

        AvroTopic<?, ?> topic = (AvroTopic<?, ?>) o;

        return keyClass == topic.getKeyClass() && valueClass == topic.getValueClass();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), keyClass, valueClass);
    }
}

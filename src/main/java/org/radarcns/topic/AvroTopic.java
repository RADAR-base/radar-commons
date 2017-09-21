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

package org.radarcns.topic;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;

/** AvroTopic with schema. */
public class AvroTopic<K, V> extends KafkaTopic {
    private final Schema valueSchema;
    private final Schema keySchema;
    private final Schema.Type[] valueFieldTypes;
    private final Class<V> valueClass;
    private final Class<K> keyClass;

    public AvroTopic(String name,
            Schema keySchema, Schema valueSchema,
            Class<K> keyClass, Class<V> valueClass) {
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

    public Schema getKeySchema() {
        return keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    public Class<K> getKeyClass() {
        return keyClass;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }

        AvroTopic topic = (AvroTopic) o;

        return keyClass == topic.getKeyClass() && valueClass == topic.getValueClass();
    }

    /**
     * Parse an AvroTopic from the values in this class.
     */
    @SuppressWarnings({"unchecked", "JavaReflectionMemberAccess"})
    public static <K extends SpecificRecord, V extends SpecificRecord> AvroTopic<K, V> parse(
            String topic, String keySchema, String valueSchema)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {

        Objects.requireNonNull(topic, "topic needs to be specified");
        Objects.requireNonNull(keySchema, "key_schema needs to be specified");
        Objects.requireNonNull(valueSchema, "value_schema needs to be specified");

        Class<K> keyClass = (Class<K>) Class.forName(keySchema);
        Schema keyAvroSchema = (Schema) keyClass
                .getMethod("getClassSchema").invoke(null);
        // check instantiation
        SpecificData.newInstance(keyClass, keyAvroSchema);

        Class<V> valueClass = (Class<V>) Class.forName(valueSchema);
        Schema valueAvroSchema = (Schema) valueClass
                .getMethod("getClassSchema").invoke(null);
        // check instantiation
        SpecificData.newInstance(valueClass, valueAvroSchema);

        return new AvroTopic<>(topic, keyAvroSchema, valueAvroSchema, keyClass, valueClass);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + keyClass.hashCode();
        result = 31 * result + valueClass.hashCode();
        return result;
    }
}

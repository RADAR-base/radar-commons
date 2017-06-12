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

package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.topic.AvroTopic;

/**
 * Specifies an Avro topic
 */
public class AvroTopicConfig {
    private String topic;
    @JsonProperty("key_schema")
    private String keySchema;
    @JsonProperty("value_schema")
    private String valueSchema;
    private List<String> tags;

    /**
     * Parse an AvroTopic from the values in this class.
     */
    @SuppressWarnings("unchecked")
    public <K extends SpecificRecord, V extends SpecificRecord> AvroTopic<K, V> parseAvroTopic()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {

        Objects.requireNonNull(this.topic, "topic needs to be specified");
        Objects.requireNonNull(this.keySchema, "key_schema needs to be specified");
        Objects.requireNonNull(this.valueSchema, "value_schema needs to be specified");

        Class<K> keyClass = (Class<K>) Class.forName(this.keySchema);
        Schema keyAvroSchema = (Schema) keyClass
                .getMethod("getClassSchema").invoke(null);
        // check instantiation
        SpecificData.newInstance(keyClass, keyAvroSchema);

        Class<V> valueClass = (Class<V>) Class.forName(this.valueSchema);
        Schema valueAvroSchema = (Schema) valueClass
                .getMethod("getClassSchema").invoke(null);
        // check instantiation
        SpecificData.newInstance(valueClass, valueAvroSchema);

        return new AvroTopic<>(topic, keyAvroSchema, valueAvroSchema, keyClass, valueClass);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(String keySchema) {
        this.keySchema = keySchema;
    }

    public String getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(String valueSchema) {
        this.valueSchema = valueSchema;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}

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

package org.radarcns.producer.rest;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.radarcns.data.AvroEncoder;
import org.radarcns.data.Record;
import org.radarcns.topic.AvroTopic;

/**
 * Request data to submit records to the Kafka REST proxy.
 */
class TopicRequestData<K, V> {
    private final AvroEncoder.AvroWriter<K> keyWriter;
    private final AvroEncoder.AvroWriter<V> valueWriter;
    private final JsonFactory jsonFactory;

    private Integer keySchemaId;
    private Integer valueSchemaId;
    private String keySchemaString;
    private String valueSchemaString;

    private List<Record<K, V>> records;

    TopicRequestData(AvroTopic<K, V> topic, AvroEncoder keyEncoder, AvroEncoder valueEncoder,
            JsonFactory jsonFactory) throws IOException {
        keyWriter = keyEncoder.writer(topic.getKeySchema(), topic.getKeyClass());
        valueWriter = valueEncoder.writer(topic.getValueSchema(), topic.getValueClass());
        this.jsonFactory = jsonFactory;
    }

    void writeToStream(OutputStream out) throws IOException {
        try (JsonGenerator writer = jsonFactory.createGenerator(out, JsonEncoding.UTF8)) {
            writer.writeStartObject();
            if (keySchemaId != null) {
                writer.writeNumberField("key_schema_id", keySchemaId);
            } else {
                writer.writeStringField("key_schema", keySchemaString);
            }

            if (valueSchemaId != null) {
                writer.writeNumberField("value_schema_id", valueSchemaId);
            } else {
                writer.writeStringField("value_schema", valueSchemaString);
            }

            writer.writeArrayFieldStart("records");

            for (Record<K, V> record : records) {
                writer.writeStartObject();
                writer.writeFieldName("key");
                writer.writeRawValue(new String(keyWriter.encode(record.key)));
                writer.writeFieldName("value");
                writer.writeRawValue(new String(valueWriter.encode(record.value)));
                writer.writeEndObject();
            }
            writer.writeEndArray();
            writer.writeEndObject();
        }
    }

    void reset() {
        keySchemaId = null;
        keySchemaString = null;
        valueSchemaId = null;
        valueSchemaString = null;
        records = null;
    }

    void setKeySchemaId(Integer keySchemaId) {
        this.keySchemaId = keySchemaId;
    }

    void setValueSchemaId(Integer valueSchemaId) {
        this.valueSchemaId = valueSchemaId;
    }

    void setKeySchemaString(String keySchemaString) {
        this.keySchemaString = keySchemaString;
    }

    void setValueSchemaString(String valueSchemaString) {
        this.valueSchemaString = valueSchemaString;
    }

    void setRecords(List<Record<K, V>> records) {
        this.records = records;
    }

    Integer getKeySchemaId() {
        return keySchemaId;
    }

    Integer getValueSchemaId() {
        return valueSchemaId;
    }
}

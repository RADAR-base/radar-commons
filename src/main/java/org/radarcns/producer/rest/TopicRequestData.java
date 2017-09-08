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

import org.json.JSONObject;
import org.radarcns.data.AvroEncoder;
import org.radarcns.data.Record;
import org.radarcns.topic.AvroTopic;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

/**
 * Request data to submit records to the Kafka REST proxy.
 */
class TopicRequestData<K, V> {
    private final AvroEncoder.AvroWriter<K> keyWriter;
    private final AvroEncoder.AvroWriter<V> valueWriter;

    private Integer keySchemaId;
    private Integer valueSchemaId;
    private String keySchemaString;
    private String valueSchemaString;

    private List<Record<K, V>> records;

    TopicRequestData(AvroTopic<K, V> topic, AvroEncoder keyEncoder, AvroEncoder valueEncoder)
            throws IOException {
        keyWriter = keyEncoder.writer(topic.getKeySchema(), topic.getKeyClass());
        valueWriter = valueEncoder.writer(topic.getValueSchema(), topic.getValueClass());
    }

    void writeToStream(OutputStream out) throws IOException {
        try (BufferedOutputStream buf = new BufferedOutputStream(out);
                OutputStreamWriter writer = new OutputStreamWriter(buf)) {
            writer.append('{');
            if (keySchemaId != null) {
                writer.append("\"key_schema_id\":").append(String.valueOf(keySchemaId));
            } else {
                writer.append("\"key_schema\":");
                JSONObject.quote(keySchemaString, writer);
            }
            if (valueSchemaId != null) {
                writer.append(",\"value_schema_id\":").append(String.valueOf(valueSchemaId));
            } else {
                writer.append(",\"value_schema\":");
                JSONObject.quote(valueSchemaString, writer);
            }
            writer.append(",\"records\":[");

            for (int i = 0; i < records.size(); i++) {
                Record<K, V> record = records.get(i);

                if (i == 0) {
                    writer.append("{\"key\":");
                } else {
                    writer.append(",{\"key\":");
                }
                writer.flush();
                buf.write(keyWriter.encode(record.key));
                writer.append(",\"value\":");
                writer.flush();
                buf.write(valueWriter.encode(record.value));
                writer.append('}');
            }
            writer.append("]}");
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

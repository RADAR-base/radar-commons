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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Request data to submit records to the Kafka REST proxy.
 */
class TopicRequestData<K, V> {
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final byte[] KEY_SCHEMA_ID = "\"key_schema_id\":".getBytes(UTF_8);
    private static final byte[] KEY_SCHEMA = "\"key_schema\":".getBytes(UTF_8);
    private static final byte[] VALUE_SCHEMA_ID = ",\"value_schema_id\":".getBytes(UTF_8);
    private static final byte[] VALUE_SCHEMA = ",\"value_schema\":".getBytes(UTF_8);
    private static final byte[] RECORDS = ",\"records\":[".getBytes(UTF_8);
    private static final byte[] KEY = "{\"key\":".getBytes(UTF_8);
    private static final byte[] VALUE = ",\"value\":".getBytes(UTF_8);
    private static final byte[] END = "]}".getBytes(UTF_8);

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

    /**
     * Writes the current topic to a stream. This implementation does not use any JSON writers to
     * write the data, but writes it directly to a stream. {@link JSONObject#quote(String)}
     * is used to get the correct formatting. This makes the method as lean as possible.
     * @param out OutputStream to write to. It is assumed to be buffered.
     * @throws IOException if a superimposing stream could not be created
     */
    void writeToStream(OutputStream out) throws IOException {
        out.write('{');
        if (keySchemaId != null) {
            out.write(KEY_SCHEMA_ID);
            out.write(keySchemaId.toString().getBytes(UTF_8));
        } else {
            out.write(KEY_SCHEMA);
            out.write(JSONObject.quote(keySchemaString).getBytes(UTF_8));
        }
        if (valueSchemaId != null) {
            out.write(VALUE_SCHEMA_ID);
            out.write(valueSchemaId.toString().getBytes(UTF_8));
        } else {
            out.write(VALUE_SCHEMA);
            out.write(JSONObject.quote(valueSchemaString).getBytes(UTF_8));
        }

        out.write(RECORDS);

        for (int i = 0; i < records.size(); i++) {
            Record<K, V> record = records.get(i);

            if (i > 0) {
                out.write(',');
            }
            out.write(KEY);
            out.write(keyWriter.encode(record.key));

            out.write(VALUE);
            out.write(valueWriter.encode(record.value));
            out.write('}');
        }
        out.write(END);
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

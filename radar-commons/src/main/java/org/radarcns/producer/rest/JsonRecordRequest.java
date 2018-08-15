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
import org.radarcns.data.AvroRecordData;
import org.radarcns.data.RecordData;
import org.radarcns.topic.AvroTopic;

import java.io.IOException;
import java.io.OutputStream;

import static org.radarcns.util.Strings.utf8;

/**
 * Request data to submit records to the Kafka REST proxy.
 */
class JsonRecordRequest<K, V> implements RecordRequest<K, V> {
    public static final byte[] KEY_SCHEMA_ID = utf8("\"key_schema_id\":");
    public static final byte[] VALUE_SCHEMA_ID = utf8(",\"value_schema_id\":");
    public static final byte[] RECORDS = utf8(",\"records\":[");
    public static final byte[] KEY = utf8("{\"key\":");
    public static final byte[] VALUE = utf8(",\"value\":");
    public static final byte[] END = utf8("]}");

    private final AvroEncoder.AvroWriter<K> keyEncoder;
    private final AvroEncoder.AvroWriter<V> valueEncoder;

    private int keySchemaId;
    private int valueSchemaId;
    private RecordData<K, V> records;

    public JsonRecordRequest(AvroTopic<K, V> topic) {
        try {
            keyEncoder = AvroRecordData.getEncoder(
                    topic.getKeySchema(), topic.getKeyClass(), false);
            valueEncoder = AvroRecordData.getEncoder(
                    topic.getValueSchema(), topic.getValueClass(), false);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Cannot create encoder of schema.", ex);
        }
    }

    /**
     * Writes the current topic to a stream. This implementation does not use any JSON writers to
     * write the data, but writes it directly to a stream. {@link JSONObject#quote(String)}
     * is used to get the correct formatting. This makes the method as lean as possible.
     * @param out OutputStream to write to. It is assumed to be buffered.
     * @throws IOException if a superimposing stream could not be created
     */
    @Override
    public void writeToStream(OutputStream out) throws IOException {
        out.write('{');
        out.write(KEY_SCHEMA_ID);
        out.write(utf8(String.valueOf(keySchemaId)));
        out.write(VALUE_SCHEMA_ID);
        out.write(utf8(String.valueOf(valueSchemaId)));

        out.write(RECORDS);

        byte[] key = keyEncoder.encode(records.getKey());

        boolean first = true;
        for (V record : records.values()) {
            if (first) {
                first = false;
            } else {
                out.write(',');
            }
            out.write(KEY);
            out.write(key);

            out.write(VALUE);
            out.write(valueEncoder.encode(record));
            out.write('}');
        }
        out.write(END);
    }

    @Override
    public void reset() {
        records = null;
    }

    @Override
    public void setKeySchemaMetadata(ParsedSchemaMetadata schema) {
        this.keySchemaId = schema.getId();
    }

    @Override
    public void setValueSchemaMetadata(ParsedSchemaMetadata schema) {
        this.valueSchemaId = schema.getId();
    }

    @Override
    public void setRecords(RecordData<K, V> records) {
        this.records = records;
    }
}

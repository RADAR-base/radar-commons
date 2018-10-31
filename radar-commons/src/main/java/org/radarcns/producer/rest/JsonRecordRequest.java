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

import static org.radarcns.util.Strings.utf8;

import java.io.IOException;
import okio.Buffer;
import okio.BufferedSink;
import org.apache.avro.SchemaValidationException;
import org.json.JSONObject;
import org.radarcns.data.AvroEncoder.AvroWriter;
import org.radarcns.data.RecordData;
import org.radarcns.data.RemoteSchemaEncoder;
import org.radarcns.topic.AvroTopic;

/**
 * Request data to submit records to the Kafka REST proxy.
 */
public class JsonRecordRequest<K, V> implements RecordRequest<K, V> {
    public static final byte[] KEY_SCHEMA_ID = utf8("\"key_schema_id\":");
    public static final byte[] VALUE_SCHEMA_ID = utf8(",\"value_schema_id\":");
    public static final byte[] RECORDS = utf8(",\"records\":[");
    public static final byte[] KEY = utf8("{\"key\":");
    public static final byte[] VALUE = utf8(",\"value\":");
    public static final byte[] END = utf8("]}");

    private final AvroWriter<K> keyEncoder;
    private final AvroWriter<V> valueEncoder;

    private RecordData<K, V> records;

    /**
     * Generate a record request for given topic.
     * @param topic topic to use.
     * @throws IllegalStateException if key or value encoders could not be made.
     */
    public JsonRecordRequest(AvroTopic<K, V> topic) {
        RemoteSchemaEncoder schemaEncoder = new RemoteSchemaEncoder(false);

        this.keyEncoder = schemaEncoder.writer(topic.getKeySchema(), topic.getKeyClass());
        this.valueEncoder = schemaEncoder.writer(topic.getValueSchema(), topic.getValueClass());
    }

    /**
     * Writes the current topic to a stream. This implementation does not use any JSON writers to
     * write the data, but writes it directly to a stream. {@link JSONObject#quote(String)}
     * is used to get the correct formatting. This makes the method as lean as possible.
     * @param sink buffered sink to write to.
     * @throws IOException if a superimposing stream could not be created
     */
    @Override
    public void writeToSink(BufferedSink sink) throws IOException {
        writeToSink(sink, Integer.MAX_VALUE);
    }

    private void writeToSink(BufferedSink sink, int maxLength) throws IOException {
        sink.writeByte('{');
        sink.write(KEY_SCHEMA_ID);
        sink.write(utf8(String.valueOf(keyEncoder.getReaderSchema().getId())));
        sink.write(VALUE_SCHEMA_ID);
        sink.write(utf8(String.valueOf(valueEncoder.getReaderSchema().getId())));

        sink.write(RECORDS);

        byte[] key = keyEncoder.encode(records.getKey());

        int curLength = KEY_SCHEMA_ID.length + VALUE_SCHEMA_ID.length + 7;

        boolean first = true;
        for (V record : records) {
            if (curLength >= maxLength) {
                return;
            }
            if (first) {
                first = false;
            } else {
                sink.writeByte(',');
            }
            sink.write(KEY);
            sink.write(key);

            sink.write(VALUE);
            byte[] valueBytes = valueEncoder.encode(record);
            sink.write(valueBytes);
            sink.writeByte('}');
            curLength += 2 + key.length + KEY.length + VALUE.length + valueBytes.length;
        }
        sink.write(END);
    }

    @Override
    public void reset() {
        records = null;
    }

    @Override
    public void prepare(ParsedSchemaMetadata keySchema, ParsedSchemaMetadata valueSchema,
            RecordData<K, V> records) throws SchemaValidationException {
        keyEncoder.setReaderSchema(keySchema);
        valueEncoder.setReaderSchema(valueSchema);
        this.records = records;
    }

    @Override
    public String content(int maxLength) throws IOException {
        Buffer buffer = new Buffer();
        writeToSink(buffer);
        return buffer.readUtf8();
    }
}

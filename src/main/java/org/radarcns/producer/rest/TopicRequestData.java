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
import org.radarcns.data.RecordData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * Request data to submit records to the Kafka REST proxy.
 */
class TopicRequestData {
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final byte[] KEY_SCHEMA_ID = "\"key_schema_id\":".getBytes(UTF_8);
    private static final byte[] KEY_SCHEMA = "\"key_schema\":".getBytes(UTF_8);
    private static final byte[] VALUE_SCHEMA_ID = ",\"value_schema_id\":".getBytes(UTF_8);
    private static final byte[] VALUE_SCHEMA = ",\"value_schema\":".getBytes(UTF_8);
    private static final byte[] RECORDS = ",\"records\":[".getBytes(UTF_8);
    private static final byte[] KEY = "{\"key\":".getBytes(UTF_8);
    private static final byte[] VALUE = ",\"value\":".getBytes(UTF_8);
    private static final byte[] END = "]}".getBytes(UTF_8);

    private final byte[] buffer;

    private Integer keySchemaId;
    private Integer valueSchemaId;
    private String keySchemaString;
    private String valueSchemaString;

    private RecordData<?, ?> records;

    TopicRequestData() {
        buffer = new byte[1024];
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

        boolean first = true;
        Iterator<InputStream> iterator = records.rawIterator();
        while (iterator.hasNext()) {
            if (first) {
                first = false;
            } else {
                out.write(',');
            }
            out.write(KEY);
            copyStream(iterator.next(), out);

            out.write(VALUE);
            copyStream(iterator.next(), out);
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

    void setRecords(RecordData<?, ?> records) {
        this.records = records;
    }

    Integer getKeySchemaId() {
        return keySchemaId;
    }

    Integer getValueSchemaId() {
        return valueSchemaId;
    }

    private void copyStream(InputStream in, OutputStream out) throws IOException {
        int len = in.read(buffer);
        while (len != -1) {
            out.write(buffer, 0, len);
            len = in.read(buffer);
        }
    }
}

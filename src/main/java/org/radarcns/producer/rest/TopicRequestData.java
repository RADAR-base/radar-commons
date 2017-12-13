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
import java.util.Iterator;

import static org.radarcns.util.Strings.utf8;

/**
 * Request data to submit records to the Kafka REST proxy.
 */
class TopicRequestData {
    private static final byte[] KEY_SCHEMA_ID = utf8("\"key_schema_id\":");
    private static final byte[] VALUE_SCHEMA_ID = utf8(",\"value_schema_id\":");
    private static final byte[] RECORDS = utf8(",\"records\":[");
    private static final byte[] KEY = utf8("{\"key\":");
    private static final byte[] VALUE = utf8(",\"value\":");
    private static final byte[] END = utf8("]}");

    private final byte[] buffer;

    private int keySchemaId;
    private int valueSchemaId;

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
        out.write(KEY_SCHEMA_ID);
        out.write(utf8(String.valueOf(keySchemaId)));
        out.write(VALUE_SCHEMA_ID);
        out.write(utf8(String.valueOf(valueSchemaId)));

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
        records = null;
    }

    void setKeySchemaId(int keySchemaId) {
        this.keySchemaId = keySchemaId;
    }

    void setValueSchemaId(int valueSchemaId) {
        this.valueSchemaId = valueSchemaId;
    }

    void setRecords(RecordData<?, ?> records) {
        this.records = records;
    }

    private void copyStream(InputStream in, OutputStream out) throws IOException {
        int len = in.read(buffer);
        while (len != -1) {
            out.write(buffer, 0, len);
            len = in.read(buffer);
        }
    }
}

/*
 * Copyright 2018 The Hyve
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

import java.io.IOException;
import okio.BufferedSink;
import org.apache.avro.SchemaValidationException;
import org.radarcns.data.RecordData;

/**
 * Record request contents. Before {@link #writeToSink(BufferedSink)} is called, first
 * {@link #prepare(ParsedSchemaMetadata, ParsedSchemaMetadata, RecordData)} should be called. This
 * class may be reused by calling prepare and reset alternatively.
 *
 * @param <K> record key type.
 * @param <V> record content type.
 */
public interface RecordRequest<K, V> {
    /** Write the current records to a stream as a request. */
    void writeToSink(BufferedSink sink) throws IOException;

    /** Reset the contents. This may free up some memory because the recordrequest may be stored. */
    void reset();

    /** Set the records to be sent. */
    void prepare(ParsedSchemaMetadata keySchema, ParsedSchemaMetadata valueSchema,
            RecordData<K, V> records) throws IOException, SchemaValidationException;

    /**
     * Return the content of the record as a string. To avoid dual reading of data for RecordData
     * that does not store the results, prepare and reset may be called around this method.
     * @param maxLength maximum returned length
     * @return the content.
     * @throws IOException if the content cannot be written.
     */
    String content(int maxLength) throws IOException;
}

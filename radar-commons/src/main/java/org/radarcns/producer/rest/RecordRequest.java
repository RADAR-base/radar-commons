package org.radarcns.producer.rest;

import okio.BufferedSink;
import org.radarcns.data.RecordData;

import java.io.IOException;

/**
 * Record request contents. Before {@link #writeToSink(BufferedSink)} is called, first
 * {@link #prepare(ParsedSchemaMetadata, ParsedSchemaMetadata, RecordData)} should be called.
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
    void prepare(ParsedSchemaMetadata keySchema, ParsedSchemaMetadata valueSchema, RecordData<K, V> records);
}

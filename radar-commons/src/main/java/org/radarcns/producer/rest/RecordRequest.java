package org.radarcns.producer.rest;

import org.radarcns.data.RecordData;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Record request contents. Before {@link #writeToStream(OutputStream)} is called, first
 * {@link #setKeySchemaMetadata(ParsedSchemaMetadata)},
 * {@link #setValueSchemaMetadata(ParsedSchemaMetadata)} and
 * {@link #setRecords(RecordData)} should be called.
 *
 * @param <K> record key type.
 * @param <V> record content type.
 */
public interface RecordRequest<K, V> {
    /** Write the current records to a stream as a request. */
    void writeToStream(OutputStream out) throws IOException;

    /** Reset the contents. This may free up some memory because the recordrequest may be stored. */
    void reset();

    /** Set the key schema metadata as returned from the schema registry. */
    void setKeySchemaMetadata(ParsedSchemaMetadata schema);

    /** Set the value schema metadata as returned from the schema registry. */
    void setValueSchemaMetadata(ParsedSchemaMetadata schema);

    /** Set the records to be sent. */
    void setRecords(RecordData<K, V> records);
}

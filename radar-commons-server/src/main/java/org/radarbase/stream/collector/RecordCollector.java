package org.radarbase.stream.collector;

import org.apache.avro.generic.IndexedRecord;

public interface RecordCollector {
    /**
     * Add a sample to the collection.
     * @param record new sample that has to be analysed
     * @throws IllegalStateException if this collector was constructed without a schema.
     */
    RecordCollector add(IndexedRecord record);
}

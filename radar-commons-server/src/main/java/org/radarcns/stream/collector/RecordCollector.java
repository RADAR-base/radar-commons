package org.radarcns.stream.collector;

import org.apache.avro.specific.SpecificRecord;

public interface RecordCollector {
    /**
     * Add a sample to the collection.
     * @param record new sample that has to be analysed
     * @throws IllegalStateException if this collector was constructed without a schema.
     */
    RecordCollector add(SpecificRecord record);
}

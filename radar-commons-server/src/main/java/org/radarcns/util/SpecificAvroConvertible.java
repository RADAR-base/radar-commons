package org.radarcns.util;

import org.apache.avro.specific.SpecificRecord;

/**
 * Convertible from Avro. This assumes that the entire state of an object can be replaced
 * with the data in the record, using e.g. an empty constructor.
 */
public interface SpecificAvroConvertible {
    /** Converts the convertible to a specificrecord. */
    SpecificRecord toAvro();

    /** Reads the convertible from a specificrecord. */
    void fromAvro(SpecificRecord record);
}

package org.radarbase.util

import org.apache.avro.specific.SpecificRecord

/**
 * Convertible from Avro. This assumes that the entire state of an object can be replaced
 * with the data in the record, using e.g. an empty constructor.
 */
interface SpecificAvroConvertible {
    /** Converts the convertible to a specificrecord.  */
    fun toAvro(): SpecificRecord

    /** Reads the convertible from a specificrecord.  */
    fun fromAvro(record: SpecificRecord)
}

package org.radarcns.producer.rest;

import org.apache.avro.Schema;

/**
 * Maps data from one avro record schema to another. Create it by calling
 * {@link AvroDataMapperFactory#createMapper(Schema, Schema, Object)}.
 */
public interface AvroDataMapper {
    /**
     * Convert an Avro GenericData to another Avro GenericData representation.
     * @param object Avro object
     * @return Avro object
     */
    Object convertAvro(Object object);
}

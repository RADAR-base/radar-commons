package org.radarbase.producer.rest

/**
 * Maps data from one avro record schema to another. Create it by calling
 * [AvroDataMapperFactory.createMapper].
 */
fun interface AvroDataMapper {
    /**
     * Convert an Avro GenericData to another Avro GenericData representation.
     * @param object Avro object
     * @return Avro object
     */
    fun convertAvro(`object`: Any?): Any?
}

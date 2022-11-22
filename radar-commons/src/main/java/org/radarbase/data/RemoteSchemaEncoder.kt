package org.radarbase.data

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificRecord
import org.radarbase.data.AvroEncoder.AvroWriter
import org.radarbase.producer.avro.AvroDataMapper
import org.radarbase.producer.avro.AvroDataMapperFactory
import org.radarbase.producer.avro.AvroDataMapperFactory.validationException
import java.io.IOException

/**
 * Encodes data according to an Avro schema to the format and schema of the server.
 *
 * @param binary true if the server wants binary encoding, false if it wants JSON encoding.
 */
class RemoteSchemaEncoder(
    private val binary: Boolean,
) : AvroEncoder {
    override fun <T: Any> writer(schema: Schema, clazz: Class<out T>, readerSchema: Schema): AvroWriter<T> {
        return SchemaEncoderWriter(binary, schema, clazz, readerSchema)
    }

    class SchemaEncoderWriter<T: Any>(
        binary: Boolean,
        schema: Schema,
        clazz: Class<out T>,
        readerSchema: Schema,
    ) : AvroWriter<T> {
        private val recordEncoder: AvroEncoder
        private val encoder: AvroWriter<Any>
        private val isGeneric: Boolean
        private val mapper: AvroDataMapper

        init {
            if (schema.type !== Schema.Type.RECORD) throw validationException(schema, readerSchema, "Can only map records.")
            val genericData: GenericData
            val classLoader = Thread.currentThread().contextClassLoader
            val useReaderSchema: Schema
            if (SpecificRecord::class.java.isAssignableFrom(clazz)) {
                genericData = SpecificData(classLoader)
                useReaderSchema = schema
                isGeneric = false
            } else {
                genericData = GenericData(classLoader)
                useReaderSchema = readerSchema
                isGeneric = true
            }
            recordEncoder = AvroDatumEncoder(genericData, binary)
            mapper = AvroDataMapperFactory.createMapper(schema, useReaderSchema, null)
            encoder = recordEncoder.writer(useReaderSchema, Any::class.java)
        }

        @Throws(IOException::class)
        override fun encode(`object`: T): ByteArray = encoder.encode(
            requireNotNull(mapper.convertAvro(`object`)) {
                "Cannot map $`object` to Avro"
            }
        )
    }
}

package org.radarbase.data

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificRecord
import org.radarbase.data.AvroEncoder.AvroWriter
import org.radarbase.producer.rest.AvroDataMapper
import org.radarbase.producer.rest.AvroDataMapperFactory
import org.radarbase.producer.rest.ParsedSchemaMetadata
import java.io.IOException

/**
 * Encodes data according to an Avro schema to the format and schema of the server.
 *
 * @param binary true if the server wants binary encoding, false if it wants JSON encoding.
 */
class RemoteSchemaEncoder(
    private val binary: Boolean,
) : AvroEncoder {
    override fun <T: Any> writer(schema: Schema, clazz: Class<out T>): AvroWriter<T> {
        return SchemaEncoderWriter(schema, clazz)
    }

    private inner class SchemaEncoderWriter<T: Any>(
        private val schema: Schema,
        clazz: Class<out T>
    ) : AvroWriter<T> {
        private val recordEncoder: AvroEncoder
        private var encoder: AvroWriter<Any>? = null
        private var isGeneric = false
        override var readerSchema: ParsedSchemaMetadata? = null
            set(value) {
                value ?: return
                val currentField = field
                if (currentField != null && value.schema == currentField.schema) {
                    return
                }
                try {
                    if (!isGeneric) {
                        mapper = AvroDataMapperFactory.IDENTITY_MAPPER
                        encoder = recordEncoder.writer(schema, Any::class.java)
                    } else {
                        mapper = AvroDataMapperFactory.instance.createMapper(
                            schema,
                            value.schema,
                            null
                        )
                        encoder = recordEncoder.writer(value.schema, Any::class.java)
                    }
                    field = value
                } catch (ex: IOException) {
                    throw IllegalStateException("Cannot construct Avro writer", ex)
                }
            }

        private var mapper: AvroDataMapper? = null

        init {
            val genericData: GenericData
            val classLoader = Thread.currentThread().contextClassLoader
            if (SpecificRecord::class.java.isAssignableFrom(clazz)) {
                genericData = SpecificData(classLoader)
                isGeneric = false
            } else {
                genericData = GenericData(classLoader)
                isGeneric = true
            }
            recordEncoder = AvroDatumEncoder(genericData, binary)
        }

        @Throws(IOException::class)
        override fun encode(`object`: T): ByteArray {
            val localEncoder = checkNotNull(encoder) { "Did not initialize reader schema yet " }
            val localMapper = checkNotNull(mapper) { "Did not initialize reader schema yet" }
            return localEncoder.encode(
                requireNotNull(localMapper.convertAvro(`object`)) {
                    "Cannot map $`object` to Avro"
                }
            )
        }
    }
}

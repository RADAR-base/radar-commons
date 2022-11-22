package org.radarbase.producer.rest

import io.ktor.http.content.*
import io.ktor.utils.io.*
import org.radarbase.data.RecordData
import org.radarbase.data.RemoteSchemaEncoder
import org.radarbase.producer.io.FunctionalWriteChannelContent
import org.radarbase.producer.schema.ParsedSchemaMetadata
import org.slf4j.LoggerFactory

class JsonRecordContent<K: Any, V: Any>(
    private val records: RecordData<K, V>,
    private val keySchemaMetadata: ParsedSchemaMetadata,
    private val valueSchemaMetadata: ParsedSchemaMetadata,
) : AvroRecordContent {
    private val keyEncoder = RemoteSchemaEncoder.SchemaEncoderWriter(
        binary = false,
        schema = records.topic.keySchema,
        clazz = records.topic.keyClass,
        readerSchema = keySchemaMetadata.schema
    )
    private val valueEncoder = RemoteSchemaEncoder.SchemaEncoderWriter(
        binary = false,
        schema = records.topic.valueSchema,
        clazz = records.topic.valueClass,
        readerSchema = valueSchemaMetadata.schema
    )

    override fun createContent(): OutgoingContent =
        FunctionalWriteChannelContent { it.writeRecords() }

    private suspend fun ByteWriteChannel.writeRecords() {
        writeByte('{'.code)
        writeFully(KEY_SCHEMA_ID)
        writeFully(keySchemaMetadata.id.toString().toByteArray())
        writeFully(VALUE_SCHEMA_ID)
        writeFully(valueSchemaMetadata.id.toString().toByteArray())
        writeFully(RECORDS)
        val key = keyEncoder.encode(records.key)
        var first = true
        for (record in records) {
            if (first) {
                first = false
            } else {
                writeByte(','.code)
            }
            writeFully(KEY)
            writeFully(key)
            writeFully(VALUE)
            writeFully(valueEncoder.encode(record))
            writeByte('}'.code)
        }
        writeFully(END)
    }

    companion object {
        val KEY_SCHEMA_ID = "\"key_schema_id\":".toByteArray()
        val VALUE_SCHEMA_ID = ",\"value_schema_id\":".toByteArray()
        val RECORDS = ",\"records\":[".toByteArray()
        val KEY = "{\"key\":".toByteArray()
        val VALUE = ",\"value\":".toByteArray()
        val END = "]}".toByteArray()

        private val logger = LoggerFactory.getLogger(JsonRecordContent::class.java)
    }
}

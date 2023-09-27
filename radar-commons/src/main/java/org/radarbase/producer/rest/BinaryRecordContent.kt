package org.radarbase.producer.rest

import io.ktor.http.ContentType
import io.ktor.http.content.OutgoingContent
import org.radarbase.data.RecordData
import org.radarbase.data.RemoteSchemaEncoder
import org.radarbase.producer.avro.AvroDataMapperFactory
import org.radarbase.producer.io.BinaryEncoder
import org.radarbase.producer.io.DirectBinaryEncoder
import org.radarbase.producer.io.FunctionalWriteChannelContent
import org.radarbase.producer.schema.ParsedSchemaMetadata
import org.slf4j.LoggerFactory

class BinaryRecordContent<V : Any>(
    private val records: RecordData<*, V>,
    keySchemaMetadata: ParsedSchemaMetadata,
    valueSchemaMetadata: ParsedSchemaMetadata,
) : AvroRecordContent {
    private val valueEncoder = RemoteSchemaEncoder.SchemaEncoderWriter(
        binary = true,
        schema = records.topic.valueSchema,
        clazz = records.topic.valueClass,
        readerSchema = valueSchemaMetadata.schema,
    )
    private val sourceId = records.sourceId
        ?: throw AvroDataMapperFactory.validationException(
            records.topic.keySchema,
            keySchemaMetadata.schema,
            "Cannot map record without source ID",
        )

    private val keySchemaVersion = requireNotNull(keySchemaMetadata.version) {
        "missing key schema version"
    }
    private val valueSchemaVersion = requireNotNull(valueSchemaMetadata.version) {
        "missing key schema version"
    }

    override fun createContent(contentType: ContentType): OutgoingContent =
        FunctionalWriteChannelContent(contentType) { channel ->
            DirectBinaryEncoder(channel).use {
                it.writeRecords()
            }
        }

    private suspend fun BinaryEncoder.writeRecords() {
        startItem()
        writeInt(keySchemaVersion)
        writeInt(valueSchemaVersion)

        // do not send project ID; it is encoded in the serialization
        writeIndex(0)
        // do not send user ID; it is encoded in the serialization
        writeIndex(0)
        writeString(sourceId)
        writeArrayStart()
        setItemCount(records.size().toLong())
        for (record in records) {
            startItem()
            writeBytes(valueEncoder.encode(record))
        }
        writeArrayEnd()
        flush()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BinaryRecordContent::class.java)
    }
}

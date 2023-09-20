package org.radarbase.producer.rest

import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.serialization.*
import io.ktor.util.reflect.*
import io.ktor.utils.io.*
import io.ktor.utils.io.charsets.*
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.radarbase.data.RecordData
import org.radarbase.producer.schema.SchemaRetriever

class AvroContentConverter(
    private val schemaRetriever: SchemaRetriever,
    private val binary: Boolean,
) : ContentConverter {
    override suspend fun serializeNullable(
        contentType: ContentType,
        charset: Charset,
        typeInfo: TypeInfo,
        value: Any?,
    ): OutgoingContent? {
        if (value !is RecordData<*, *>) return null

        return coroutineScope {
            val keySchema = async {
                schemaRetriever.metadata(
                    topic = value.topic.name,
                    ofValue = false,
                    schema = value.topic.keySchema,
                )
            }
            val valueSchema = async {
                schemaRetriever.metadata(
                    topic = value.topic.name,
                    ofValue = true,
                    schema = value.topic.valueSchema,
                )
            }
            val maker = if (binary) {
                BinaryRecordContent(
                    records = value,
                    keySchemaMetadata = keySchema.await(),
                    valueSchemaMetadata = valueSchema.await(),
                )
            } else {
                JsonRecordContent(
                    records = value,
                    keySchemaMetadata = keySchema.await(),
                    valueSchemaMetadata = valueSchema.await(),
                )
            }
            maker.createContent(contentType)
        }
    }

    override suspend fun deserialize(
        charset: Charset,
        typeInfo: TypeInfo,
        content: ByteReadChannel,
    ): Any? = null
}

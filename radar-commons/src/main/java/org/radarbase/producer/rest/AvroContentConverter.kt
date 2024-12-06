package org.radarbase.producer.rest

import io.ktor.http.ContentType
import io.ktor.http.content.OutgoingContent
import io.ktor.serialization.ContentConverter
import io.ktor.util.reflect.TypeInfo
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.charsets.Charset
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
                schemaRetriever.getByVersion(
                    topic = value.topic.name,
                    ofValue = false,
                    version = -1,
                )
            }
            val valueSchema = async {
                schemaRetriever.getByVersion(
                    topic = value.topic.name,
                    ofValue = true,
                    version = -1,
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

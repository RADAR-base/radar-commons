package org.radarbase.producer.io

import io.ktor.http.ContentType
import io.ktor.http.content.OutgoingContent
import io.ktor.utils.io.ByteWriteChannel

class FunctionalWriteChannelContent(
    override val contentType: ContentType,
    private val writeAction: suspend (ByteWriteChannel) -> Unit,
) : OutgoingContent.WriteChannelContent() {
    override suspend fun writeTo(channel: ByteWriteChannel) = writeAction(channel)
}

package org.radarbase.producer.io

import io.ktor.http.content.*
import io.ktor.utils.io.*

class FunctionalWriteChannelContent(
    private val writeAction: suspend (ByteWriteChannel) -> Unit,
) : OutgoingContent.WriteChannelContent() {
    override suspend fun writeTo(channel: ByteWriteChannel) = writeAction(channel)
}

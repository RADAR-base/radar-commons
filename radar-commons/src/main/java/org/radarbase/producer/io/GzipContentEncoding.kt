package org.radarbase.producer.io

import io.ktor.client.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.util.*
import io.ktor.util.cio.*
import io.ktor.utils.io.*
import kotlinx.coroutines.coroutineScope

/**
 * A plugin that allows you to enable specified compression algorithms (such as `gzip` and `deflate`) and configure their settings.
 * This plugin serves two primary purposes:
 * - Sets the `Accept-Encoding` header with the specified quality value.
 * - Decodes content received from a server to obtain the original payload.
 *
 * You can learn more from [Content encoding](https://ktor.io/docs/content-encoding.html).
 */
class GzipContentEncoding private constructor() {
    private fun setRequestHeaders(headers: HeadersBuilder) {
        if (headers.contains(HttpHeaders.ContentEncoding)) return
        headers[HttpHeaders.ContentEncoding] = "gzip"
    }

    private fun encode(headers: Headers, content: OutgoingContent): OutgoingContent {
        val encodingHeader = (headers[HttpHeaders.ContentEncoding] ?: return content).split(",")
        if (!encodingHeader.containsIgnoreCase("gzip")) return content

        return when (content) {
            is OutgoingContent.ProtocolUpgrade, is OutgoingContent.NoContent -> content
            is OutgoingContent.ReadChannelContent -> GzipReadChannel(content.readFrom())
            is OutgoingContent.ByteArrayContent -> GzipReadChannel(ByteReadChannel(content.bytes()))
            is OutgoingContent.WriteChannelContent -> GzipWriteChannel(content)
        }
    }

    /**
     * A configuration for the [GzipContentEncoding] plugin.
     */
    @KtorDsl
    class Config

    companion object : HttpClientPlugin<Config, GzipContentEncoding> {
        override val key: AttributeKey<GzipContentEncoding> = AttributeKey("GzipHttpEncoding")

        override fun prepare(block: Config.() -> Unit): GzipContentEncoding {
            return GzipContentEncoding()
        }

        override fun install(plugin: GzipContentEncoding, scope: HttpClient) {
            scope.requestPipeline.intercept(HttpRequestPipeline.State) {
                plugin.setRequestHeaders(context.headers)
            }

            scope.requestPipeline.intercept(HttpRequestPipeline.Transform) { call ->
                val method = this.context.method
                val contentLength = context.contentLength()

                if (contentLength == 0L) return@intercept
                if (contentLength == null && (method == HttpMethod.Head || method == HttpMethod.Options)) return@intercept

                if (call !is OutgoingContent) return@intercept

                proceedWith(plugin.encode(context.headers.build(), call))
            }
        }

        private fun List<String>.containsIgnoreCase(value: String): Boolean {
            return any { el -> el.trim { it <= ' ' }.equals(value, ignoreCase = true) }
        }
    }

    private class GzipReadChannel(
        private val original: ByteReadChannel,
    ) : OutgoingContent.ReadChannelContent() {
        override fun readFrom(): ByteReadChannel =
            original.deflated(gzip = true)
    }

    private class GzipWriteChannel(
        private val content: WriteChannelContent,
    ) : OutgoingContent.WriteChannelContent() {
        override suspend fun writeTo(channel: ByteWriteChannel) {
            coroutineScope {
                channel.deflated(gzip = true, coroutineContext = coroutineContext).use {
                    content.writeTo(this)
                }
            }
        }
    }
}

/*
 * Copyright 2017 The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.radarbase.producer.rest

import io.ktor.client.HttpClient
import io.ktor.client.HttpClientConfig
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.accept
import io.ktor.client.request.head
import io.ktor.client.request.headers
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.request.url
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.client.statement.request
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HeadersBuilder
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.serialization.kotlinx.serialization
import io.ktor.util.reflect.TypeInfo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.apache.avro.SchemaValidationException
import org.radarbase.data.RecordData
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.KafkaSender
import org.radarbase.producer.KafkaTopicSender
import org.radarbase.producer.io.GzipContentEncoding
import org.radarbase.producer.io.UnsupportedMediaTypeException
import org.radarbase.producer.io.timeout
import org.radarbase.producer.io.unsafeSsl
import org.radarbase.producer.rest.RestException.Companion.toRestException
import org.radarbase.producer.schema.SchemaRetriever
import org.radarbase.topic.AvroTopic
import org.radarbase.util.RadarProducerDsl
import org.slf4j.LoggerFactory
import java.io.IOException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * RestSender sends records to the Kafka REST Proxy. It does so using an Avro JSON encoding. A new
 * sender must be constructed with [.sender] per AvroTopic. This implementation is
 * blocking and unbuffered, so flush, clear and close do not do anything.
 */
class RestKafkaSender(config: Config) : KafkaSender {
    val scope = config.scope
    private val allowUnsafe: Boolean = config.allowUnsafe
    private val contentType: ContentType = config.contentType
    val schemaRetriever: SchemaRetriever = requireNotNull(config.schemaRetriever) {
        "Missing schemaRetriever from configuration"
    }

    /** Get the current REST client.  */
    val restClient: HttpClient

    private val _connectionState: ConnectionState = config.connectionState
        ?: ConnectionState(DEFAULT_TIMEOUT, scope)

    override val connectionState: Flow<ConnectionState.State>
        get() = _connectionState.state

    private val baseUrl: String = requireNotNull(config.baseUrl).trimEnd('/')
    private val headers: Headers = config.headers.build()
    private val connectionTimeout: Duration = config.connectionTimeout
    private val contentEncoding = config.contentEncoding
    private val originalHttpClient = config.httpClient

    /**
     * Construct a RestSender.
     */
    init {
        restClient = config.httpClient?.config {
            configure()
        } ?: HttpClient(CIO) {
            configure()
        }
    }

    private fun HttpClientConfig<*>.configure() {
        timeout(connectionTimeout)
        install(ContentNegotiation) {
            register(
                KAFKA_REST_BINARY_ENCODING,
                AvroContentConverter(schemaRetriever, binary = true),
            )
            register(
                KAFKA_REST_JSON_ENCODING,
                AvroContentConverter(schemaRetriever, binary = false),
            )
            serialization(
                KAFKA_REST_ACCEPT,
                Json {
                    ignoreUnknownKeys = true
                },
            )
        }
        when (contentEncoding) {
            GZIP_CONTENT_ENCODING -> install(GzipContentEncoding)
            else -> {}
        }
        if (allowUnsafe) {
            unsafeSsl()
        }
        defaultRequest {
            url("$baseUrl/")
            contentType(contentType)
            accept(ContentType.Application.Json)
            headers {
                appendAll(this@RestKafkaSender.headers)
            }
        }
    }

    inner class RestKafkaTopicSender<K : Any, V : Any>(
        override val topic: AvroTopic<K, V>,
    ) : KafkaTopicSender<K, V> {

        val recordDataTypeInfo: TypeInfo = TypeInfo(
            type = RecordData::class,
            kotlinType = null,
            reifiedType = RadarParameterizedType(
                raw = RecordData::class.java,
                args = arrayOf(topic.keyClass, topic.valueClass),
            ),
        )

        override suspend fun send(records: RecordData<K, V>) = withContext(scope.coroutineContext) {
            try {
                val response: HttpResponse = restClient.post {
                    url("topics/${topic.name}")
                    setBody(records, recordDataTypeInfo)
                }
                if (response.status.isSuccess()) {
                    _connectionState.didConnect()
                    logger.debug("Added message to topic {}", topic)
                } else if (response.status == HttpStatusCode.Unauthorized || response.status == HttpStatusCode.Forbidden) {
                    _connectionState.wasUnauthorized()
                    throw AuthenticationException("Request unauthorized")
                } else if (response.status == HttpStatusCode.UnsupportedMediaType) {
                    throw UnsupportedMediaTypeException(
                        response.request.contentType() ?: response.request.content.contentType,
                        response.request.headers[HttpHeaders.ContentEncoding],
                    )
                } else {
                    _connectionState.didDisconnect()
                    throw response.toRestException()
                }
            } catch (ex: IOException) {
                _connectionState.didDisconnect()
                throw ex
            }
        }
    }

    @Throws(SchemaValidationException::class)
    override fun <K : Any, V : Any> sender(topic: AvroTopic<K, V>): KafkaTopicSender<K, V> {
        return RestKafkaTopicSender(topic)
    }

    @Throws(AuthenticationException::class)
    override suspend fun resetConnection(): Boolean {
        if (connectionState.first() === ConnectionState.State.CONNECTED) {
            return true
        }
        val lastState = try {
            val response = scope.async {
                restClient.head {
                    url("")
                }
            }.await()
            if (response.status.isSuccess()) {
                _connectionState.didConnect()
                ConnectionState.State.CONNECTED
            } else if (response.status == HttpStatusCode.Unauthorized) {
                _connectionState.wasUnauthorized()
                throw AuthenticationException("HEAD request unauthorized")
            } else {
                _connectionState.didDisconnect()
                val bodyString = response.bodyAsText()
                logger.warn(
                    "Failed to make heartbeat request to {} (HTTP status code {}): {}",
                    restClient,
                    response.status,
                    bodyString,
                )
                ConnectionState.State.DISCONNECTED
            }
        } catch (ex: IOException) {
            // no stack trace is needed
            _connectionState.didDisconnect()
            logger.warn("Failed to make heartbeat request to {}: {}", restClient, ex.toString())
            ConnectionState.State.DISCONNECTED
        }
        return lastState === ConnectionState.State.CONNECTED
    }

    fun config(config: Config.() -> Unit): RestKafkaSender {
        val oldConfig = toConfig()
        val newConfig = toConfig().apply(config)
        return if (oldConfig == newConfig) this else RestKafkaSender(newConfig)
    }

    private fun toConfig() = Config().apply {
        scope = this@RestKafkaSender.scope
        baseUrl = this@RestKafkaSender.baseUrl
        httpClient = this@RestKafkaSender.originalHttpClient
        schemaRetriever = this@RestKafkaSender.schemaRetriever
        headers = HeadersBuilder().apply { appendAll(this@RestKafkaSender.headers) }
        contentType = this@RestKafkaSender.contentType
        contentEncoding = this@RestKafkaSender.contentEncoding
        connectionTimeout = this@RestKafkaSender.connectionTimeout
        allowUnsafe = this@RestKafkaSender.allowUnsafe
    }

    @RadarProducerDsl
    class Config {
        var scope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        var baseUrl: String? = null
        var schemaRetriever: SchemaRetriever? = null
        var connectionState: ConnectionState? = null
        var httpClient: HttpClient? = null
        var headers = HeadersBuilder()
        var connectionTimeout: Duration = 30.seconds
        var contentEncoding: String? = null
        var allowUnsafe: Boolean = false
        var contentType: ContentType = KAFKA_REST_JSON_ENCODING

        fun httpClient(config: HttpClientConfig<*>.() -> Unit = {}) {
            httpClient = httpClient?.config(config)
                ?: HttpClient(CIO, config)
        }

        fun schemaRetriever(schemaBaseUrl: String, builder: SchemaRetriever.Config.() -> Unit = {}) {
            schemaRetriever = SchemaRetriever.schemaRetriever(schemaBaseUrl) {
                httpClient = this@Config.httpClient
                builder()
            }
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || javaClass != other.javaClass) return false
            other as Config
            return schemaRetriever == other.schemaRetriever &&
                connectionState == other.connectionState &&
                headers.build() == other.headers.build() &&
                httpClient == other.httpClient &&
                contentType == other.contentType &&
                baseUrl == other.baseUrl &&
                connectionTimeout == other.connectionTimeout &&
                contentEncoding == other.contentEncoding &&
                scope == other.scope
        }
        override fun hashCode(): Int = headers.hashCode()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RestKafkaSender::class.java)

        val DEFAULT_TIMEOUT: Duration = 20.seconds
        val KAFKA_REST_BINARY_ENCODING = ContentType("application", "vnd.radarbase.avro.v1+binary")
        val KAFKA_REST_JSON_ENCODING = ContentType("application", "vnd.kafka.avro.v2+json")
        val KAFKA_REST_ACCEPT = ContentType("application", "vnd.kafka.v2+json")
        const val GZIP_CONTENT_ENCODING = "gzip"

        fun restKafkaSender(builder: Config.() -> Unit): RestKafkaSender =
            RestKafkaSender(Config().apply(builder))
    }
}

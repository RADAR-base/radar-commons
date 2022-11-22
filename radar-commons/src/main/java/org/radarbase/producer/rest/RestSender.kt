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

import okhttp3.Headers
import okhttp3.MediaType
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Protocol
import okhttp3.Request
import org.apache.avro.SchemaValidationException
import org.radarbase.config.ServerConfig
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.KafkaSender
import org.radarbase.producer.KafkaTopicSender
import org.radarbase.producer.rest.RestClient.Companion.bodyString
import org.radarbase.producer.rest.RestClient.Companion.globalRestClient
import org.radarbase.topic.AvroTopic
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.MalformedURLException
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * RestSender sends records to the Kafka REST Proxy. It does so using an Avro JSON encoding. A new
 * sender must be constructed with [.sender] per AvroTopic. This implementation is
 * blocking and unbuffered, so flush, clear and close do not do anything.
 */
class RestSender private constructor(builder: Builder) : KafkaSender {
    /**
     * Get the current request properties.
     */
    @get:Synchronized
    var requestProperties: RequestProperties
        private set
    private var connectionTestRequest: Request.Builder
    /** Get the schema retriever.  */
    /** Set the schema retriever.  */
    @get:Synchronized
    @set:Synchronized
    var schemaRetriever: SchemaRetriever?

    /** Get the current REST client.  */
    @get:Synchronized
    var restClient: RestClient = requireNotNull(builder.httpClient).withConfiguration {
        protocols(listOf(Protocol.HTTP_1_1))
    }
        private set(value) {
            try {
                connectionTestRequest = value.requestBuilder("").head()
            } catch (ex: MalformedURLException) {
                throw IllegalArgumentException("Schemaless topics do not have a valid URL", ex)
            }
            field = value
            state.reset()
        }
    private val state: ConnectionState

    /**
     * Construct a RestSender.
     */
    init {
        schemaRetriever = Objects.requireNonNull(builder.schemaRetriever)
        requestProperties = RequestProperties(
            KAFKA_REST_ACCEPT_ENCODING,
            if (builder.useBinaryContent) KAFKA_REST_BINARY_ENCODING else KAFKA_REST_AVRO_ENCODING,
            builder.headers.build(),
            builder.useBinaryContent
        )
        state = builder.connectionState
            ?: ConnectionState(RestClient.DEFAULT_TIMEOUT, TimeUnit.SECONDS)
        restClient = builder.httpClient?.withConfiguration {
            protocols(listOf(Protocol.HTTP_1_1))
        } ?: globalRestClient {
            protocols(listOf(Protocol.HTTP_1_1))
        }
        try {
            connectionTestRequest = restClient.requestBuilder("").head()
        } catch (ex: MalformedURLException) {
            throw IllegalArgumentException("Schemaless topics do not have a valid URL", ex)
        }
    }

    /**
     * Set the connection timeout. This affects both the connection state as the HTTP client
     * setting.
     * @param connectionTimeout timeout
     * @param unit time unit
     */
    @Synchronized
    fun setConnectionTimeout(connectionTimeout: Long, unit: TimeUnit) {
        if (connectionTimeout == restClient.timeout) return
        restClient = restClient.withConfiguration {
            timeout(connectionTimeout, unit)
        }
        state.setTimeout(connectionTimeout, unit)
    }

    /**
     * Set the Kafka REST Proxy settings. This affects the REST client.
     * @param kafkaConfig server configuration of the Kafka REST proxy.
     */
    @Synchronized
    fun setKafkaConfig(kafkaConfig: ServerConfig) {
        if (kafkaConfig == restClient.server) return
        restClient = restClient.withConfiguration {
            server = kafkaConfig
        }
    }

    /** Get a request to check the connection status.  */
    @Synchronized
    private fun getConnectionTestRequest(): Request {
        return connectionTestRequest.headers(requestProperties.headers).build()
    }

    /** Set the compression of the REST client.  */
    @Synchronized
    fun setCompression(useCompression: Boolean) {
        restClient = restClient.withConfiguration {
            gzipCompression(useCompression)
        }
    }
    /** Get the headers used in requests.  */
    /** Set the headers used in requests.  */
    @get:Synchronized
    @set:Synchronized
    var headers: Headers
        get() = requestProperties.headers
        set(additionalHeaders) {
            requestProperties = RequestProperties(
                requestProperties.acceptType,
                requestProperties.contentType, additionalHeaders,
                requestProperties.binary
            )
            state.reset()
        }

    @Throws(SchemaValidationException::class)
    override fun <K: Any, V: Any> sender(topic: AvroTopic<K, V>): KafkaTopicSender<K, V> {
        return RestTopicSender(topic, this, state)
    }

    /**
     * Get the current request context.
     */
    @get:Synchronized
    val requestContext: RequestContext
        get() = RequestContext(restClient, requestProperties)

    @Throws(AuthenticationException::class)
    override fun resetConnection(): Boolean {
        if (state.state === ConnectionState.State.CONNECTED) {
            return true
        }
        try {
            restClient.request(getConnectionTestRequest()).use { response ->
                if (response.isSuccessful) {
                    state.didConnect()
                } else if (response.code == 401) {
                    state.wasUnauthorized()
                } else {
                    state.didDisconnect()
                    val bodyString = response.bodyString()
                    logger.warn(
                        "Failed to make heartbeat request to {} (HTTP status code {}): {}",
                        restClient, response.code, bodyString
                    )
                }
            }
        } catch (ex: IOException) {
            // no stack trace is needed
            state.didDisconnect()
            logger.warn("Failed to make heartbeat request to {}: {}", restClient, ex.toString())
        }
        if (state.state === ConnectionState.State.UNAUTHORIZED) {
            throw AuthenticationException("HEAD request unauthorized")
        }
        return state.state === ConnectionState.State.CONNECTED
    }

    @get:Throws(AuthenticationException::class)
    override val isConnected: Boolean
        get() = when (state.state) {
            ConnectionState.State.CONNECTED -> true
            ConnectionState.State.DISCONNECTED -> false
            ConnectionState.State.UNAUTHORIZED -> throw AuthenticationException("Unauthorized")
            ConnectionState.State.UNKNOWN -> resetConnection()
        }

    override fun close() {
        // noop
    }

    /**
     * Revert to a legacy connection if the server does not support the latest protocols.
     * @param acceptEncoding accept encoding to use in the legacy connection.
     * @param contentEncoding content encoding to use in the legacy connection.
     * @param binary whether to send the data as binary.
     */
    @Synchronized
    fun useLegacyEncoding(
        acceptEncoding: String,
        contentEncoding: MediaType,
        binary: Boolean,
    ) {
        logger.debug(
            "Reverting to encoding {} -> {} (binary: {})",
            contentEncoding, acceptEncoding, binary
        )
        requestProperties = RequestProperties(
            acceptEncoding,
            contentEncoding,
            requestProperties.headers, binary
        )
    }

    class Builder internal constructor(){
        var schemaRetriever: SchemaRetriever? = null
        var connectionState: ConnectionState? = null
        var httpClient: RestClient? = null
        var headers = Headers.Builder()
        /**
         * Whether to try to send binary content. This only works if the server supports it. If not,
         * there may be an additional round-trip.
        */
        var useBinaryContent = false

        /** Build a new RestSender.  */
        fun build(): RestSender {
            return RestSender(this)
        }
    }

    class RequestContext(
        val client: RestClient,
        val properties: RequestProperties,
    )
    class RequestProperties(
        val acceptType: String,
        val contentType: MediaType,
        val headers: Headers,
        val binary: Boolean,
    )

    companion object {
        private val logger = LoggerFactory.getLogger(RestSender::class.java)
        const val KAFKA_REST_ACCEPT_ENCODING =
            "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json"
        const val KAFKA_REST_ACCEPT_LEGACY_ENCODING =
            "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json"
        @JvmField
        val KAFKA_REST_BINARY_ENCODING: MediaType =
            "application/vnd.radarbase.avro.v1+binary".toMediaType()
        @JvmField
        val KAFKA_REST_AVRO_ENCODING: MediaType =
            "application/vnd.kafka.avro.v2+json; charset=utf-8".toMediaType()
        @JvmField
        val KAFKA_REST_AVRO_LEGACY_ENCODING: MediaType =
            "application/vnd.kafka.avro.v1+json; charset=utf-8".toMediaType()

        fun restSender(builder: Builder.() -> Unit): RestSender =
            Builder().apply(builder).build()
    }
}

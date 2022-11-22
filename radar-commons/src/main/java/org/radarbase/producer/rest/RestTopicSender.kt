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

import okhttp3.Request
import okhttp3.Response
import org.apache.avro.SchemaValidationException
import org.json.JSONException
import org.radarbase.data.AvroRecordData
import org.radarbase.data.RecordData
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.KafkaTopicSender
import org.radarbase.topic.AvroTopic
import org.slf4j.LoggerFactory
import java.io.IOException

internal class RestTopicSender<K: Any, V: Any>(
    topic: AvroTopic<K, V>,
    sender: RestSender,
    state: ConnectionState
) : KafkaTopicSender<K, V> {
    private val topic: AvroTopic<K, V>
    private var requestData: RecordRequest<K, V>
    private val sender: RestSender
    private val state: ConnectionState

    init {
        this.topic = topic
        this.sender = sender
        this.state = state
        requestData = if (sender.requestContext.properties.binary) {
            try {
                BinaryRecordRequest(topic)
            } catch (ex: IllegalArgumentException) {
                logger.warn(
                    "Cannot use Binary encoding for incompatible topic {}: {}",
                    topic, ex.toString()
                )
                JsonRecordRequest(topic)
            }
        } else {
            JsonRecordRequest(topic)
        }
    }

    @Throws(IOException::class, SchemaValidationException::class)
    override fun send(key: K, value: V) {
        send(AvroRecordData(topic, key, listOf(value)))
    }

    /**
     * Actually make a REST request to the Kafka REST server and Schema Registry.
     *
     * @param records values to send
     * @throws IOException if records could not be sent
     */
    @Throws(IOException::class, SchemaValidationException::class)
    override fun send(records: RecordData<K, V>) {
        val context = sender.requestContext
        val request = buildRequest(context, records)
        var doResend = false
        try {
            context.client.request(request).use { response ->
                if (response.isSuccessful) {
                    state.didConnect()
                    logger.debug("Added message to topic {}", topic)
                } else if (response.code == 401 || response.code == 403) {
                    state.wasUnauthorized()
                } else if (response.code == 415) {
                    downgradeConnection(request, response)
                    doResend = true
                } else {
                    throw UncheckedRequestException.fail(request, response, null)
                }
            }
        } catch (ex: IOException) {
            state.didDisconnect()
            UncheckedRequestException.fail(request, null, ex).rethrow()
        } catch (ex: UncheckedRequestException) {
            state.didDisconnect()
            ex.rethrow()
        } finally {
            requestData.reset()
        }
        if (state.state === ConnectionState.State.UNAUTHORIZED) {
            throw AuthenticationException("Request unauthorized")
        }
        if (doResend) {
            send(records)
        }
    }

    @Throws(IOException::class, SchemaValidationException::class)
    private fun updateRecords(context: RestSender.RequestContext, records: RecordData<K, V>) {
        if (!context.properties.binary && requestData is BinaryRecordRequest<*, *>) {
            requestData = JsonRecordRequest(topic)
        }
        val sendTopic = topic.name
        val retriever = sender.schemaRetriever
        val keyMetadata: ParsedSchemaMetadata
        val valueMetadata: ParsedSchemaMetadata
        try {
            keyMetadata = retriever!!.getOrSetSchemaMetadata(
                sendTopic, false, topic.keySchema, -1
            )
            valueMetadata = retriever.getOrSetSchemaMetadata(
                sendTopic, true, topic.valueSchema, -1
            )
        } catch (ex: JSONException) {
            throw IOException("Failed to get schemas for topic $topic", ex)
        } catch (ex: IOException) {
            throw IOException("Failed to get schemas for topic $topic", ex)
        }
        requestData.prepare(keyMetadata, valueMetadata, records)
    }

    @Throws(IOException::class)
    private fun downgradeConnection(request: Request, response: Response) {
        if (requestData is BinaryRecordRequest<*, *>) {
            state.didConnect()
            logger.warn(
                "Binary Avro encoding is not supported."
                        + " Switching to JSON encoding."
            )
            sender.useLegacyEncoding(
                RestSender.KAFKA_REST_ACCEPT_ENCODING, RestSender.KAFKA_REST_AVRO_ENCODING,
                false
            )
            requestData = JsonRecordRequest(topic)
        } else if (request.header("Accept") == RestSender.KAFKA_REST_ACCEPT_ENCODING) {
            state.didConnect()
            logger.warn(
                "Latest Avro encoding is not supported. Switching to legacy "
                        + "encoding."
            )
            sender.useLegacyEncoding(
                RestSender.KAFKA_REST_ACCEPT_LEGACY_ENCODING,
                RestSender.KAFKA_REST_AVRO_LEGACY_ENCODING,
                false
            )
        } else {
            val body = request.body
            val contentType = body?.contentType()
            if (contentType == null
                || contentType == RestSender.KAFKA_REST_AVRO_LEGACY_ENCODING
            ) {
                throw UncheckedRequestException.fail(
                    request, response,
                    IOException("Content-Type $contentType not accepted by server.")
                )
            } else {
                // the connection may have been downgraded already
                state.didConnect()
                logger.warn("Content-Type changed during request")
            }
        }
    }

    @Throws(IOException::class, SchemaValidationException::class)
    private fun buildRequest(
        context: RestSender.RequestContext,
        records: RecordData<K, V>
    ): Request {
        updateRecords(context, records)
        val sendToUrl = context.client.relativeUrl("topics/" + topic.name)
        val requestBody: TopicRequestBody
        val requestBuilder = Request.Builder()
            .url(sendToUrl)
            .headers(context.properties.headers)
            .header("Accept", context.properties.acceptType)
        var contentType = context.properties.contentType
        if (contentType == RestSender.KAFKA_REST_BINARY_ENCODING
            && requestData !is BinaryRecordRequest<*, *>
        ) {
            contentType = RestSender.KAFKA_REST_AVRO_ENCODING
        }
        requestBody = TopicRequestBody(requestData, contentType)
        return requestBuilder.post(requestBody).build()
    }

    override fun clear() {
        // nothing
    }

    override fun flush() {
        // nothing
    }

    override fun close() {
        // noop
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RestTopicSender::class.java)
    }
}

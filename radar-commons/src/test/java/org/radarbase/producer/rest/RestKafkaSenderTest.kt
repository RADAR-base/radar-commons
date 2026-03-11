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

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import io.ktor.util.moveToByteArray
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.apache.avro.SchemaValidationException
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.radarbase.data.AvroRecordData
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.rest.RestKafkaSender.Companion.restKafkaSender
import org.radarbase.producer.schema.ParsedSchemaMetadata
import org.radarbase.producer.schema.SchemaRetriever
import org.radarbase.topic.AvroTopic
import org.radarcns.kafka.ObservationKey
import org.radarcns.kafka.RecordSet
import org.radarcns.passive.phone.PhoneLight
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream

class RestKafkaSenderTest {
    private lateinit var retriever: SchemaRetriever
    private lateinit var sender: RestKafkaSender
    private lateinit var webServer: MockWebServer

    @BeforeEach
    fun setUp() {
        webServer = MockWebServer().apply {
            start()
        }
        retriever = mock()
        sender = restKafkaSender {
            baseUrl = webServer.url("/").toUrl().toExternalForm()
            httpClient()
            schemaRetriever = retriever
        }
    }

    @AfterEach
    fun tearDown() {
        webServer.close()
    }

    @Test
    @Throws(Exception::class)
    fun sender() = runTest {
        sender = sender.config {
            scope = this@runTest
            with(headers) {
                append("Cookie", "ab")
                append("Cookie", "bc")
            }
        }
        val keySchema = ObservationKey.getClassSchema()
        val valueSchema = PhoneLight.getClassSchema()
        val topic = AvroTopic(
            "test",
            keySchema,
            valueSchema,
            ObservationKey::class.java,
            PhoneLight::class.java,
        )
        val topicSender = sender.sender(topic)
        val key = ObservationKey("test", "a", "b")
        val value = PhoneLight(0.1, 0.2, 0.3f)
        val keySchemaMetadata = ParsedSchemaMetadata(10, 2, keySchema)
        val valueSchemaMetadata = ParsedSchemaMetadata(10, 2, valueSchema)
        retriever.stub {
            onBlocking { getByVersion("test", false, -1) }.doReturn(keySchemaMetadata)
            onBlocking { getByVersion("test", true, -1) }.doReturn(valueSchemaMetadata)
        }
        webServer.enqueueJson("{\"offset\": 100}")
        topicSender.send(key, value)
        verify(retriever, times(1))
            .getByVersion("test", false, -1)
        verify(retriever, times(1))
            .getByVersion("test", true, -1)
        val request = webServer.takeRequest()
        assertEquals("/topics/test", request.path)
        val body = READER.readTree(request.body.inputStream())
        assertEquals(10, body["key_schema_id"].asInt().toLong())
        assertEquals(10, body["value_schema_id"].asInt().toLong())
        val records = body["records"]
        assertEquals(JsonNodeType.ARRAY, records.nodeType)
        assertEquals(1, records.size().toLong())
        checkChildren(records)
        val receivedHeaders = request.headers
        assertEquals(listOf("ab; bc"), receivedHeaders.values("Cookie"))
    }

    @Test
    @Throws(Exception::class)
    fun sendBinary() = runTest {
        sender = sender.config {
            scope = this@runTest
            contentType = RestKafkaSender.KAFKA_REST_BINARY_ENCODING
        }
        val keySchema = ObservationKey.getClassSchema()
        val valueSchema = PhoneLight.getClassSchema()
        val topic = AvroTopic(
            "test",
            keySchema,
            valueSchema,
            ObservationKey::class.java,
            PhoneLight::class.java,
        )
        val topicSender = sender.sender(topic)
        val key = ObservationKey("test", "a", "b")
        val value = PhoneLight(0.1, 0.2, 0.3f)
        val keySchemaMetadata = ParsedSchemaMetadata(10, 2, keySchema)
        val valueSchemaMetadata = ParsedSchemaMetadata(10, 2, valueSchema)
        retriever.stub {
            onBlocking { getByVersion("test", false, -1) }.doReturn(keySchemaMetadata)
            onBlocking { getByVersion("test", true, -1) }.doReturn(valueSchemaMetadata)
        }
        webServer.enqueueJson("{\"offset\": 100}")
        topicSender.send(key, value)
        verify(retriever, times(1))
            .getByVersion("test", false, -1)
        verify(retriever, times(1))
            .getByVersion("test", true, -1)
        val request = webServer.takeRequest()
        assertEquals("/topics/test", request.path)
        var decoder = DecoderFactory.get().directBinaryDecoder(request.body.inputStream(), null)
        val recordSetDatumReader = SpecificDatumReader<RecordSet>(RecordSet.getClassSchema())
        val recordSet = recordSetDatumReader.read(null, decoder)
        assertNull(recordSet.userId)
        assertEquals("b", recordSet.sourceId)
        assertEquals(2, recordSet.keySchemaVersion)
        assertEquals(2, recordSet.valueSchemaVersion)
        assertEquals(1, recordSet.data.size)
        decoder = DecoderFactory.get().directBinaryDecoder(recordSet.data[0].moveToByteArray().inputStream(), decoder)
        val phoneLightDatumReader = SpecificDatumReader<PhoneLight>(PhoneLight.getClassSchema())
        val decodedValue = phoneLightDatumReader.read(null, decoder)
        assertEquals(value, decodedValue)
    }

    @Test
    @Throws(Exception::class)
    fun sendTwo() = runTest {
        sender = sender.config {
            scope = this@runTest
        }
        val keySchema = ObservationKey.getClassSchema()
        val valueSchema = PhoneLight.getClassSchema()
        val topic = AvroTopic(
            "test",
            keySchema,
            valueSchema,
            ObservationKey::class.java,
            PhoneLight::class.java,
        )
        val topicSender = sender.sender(topic)
        val key = ObservationKey("test", "a", "b")
        val value = PhoneLight(0.1, 0.2, 0.3f)
        val keySchemaMetadata = ParsedSchemaMetadata(10, 2, keySchema)
        val valueSchemaMetadata = ParsedSchemaMetadata(10, 2, valueSchema)

        retriever.stub {
            onBlocking { getByVersion("test", false, -1) }.doReturn(keySchemaMetadata)
            onBlocking { getByVersion("test", true, -1) }.doReturn(valueSchemaMetadata)
        }
        webServer.enqueueJson("{\"offset\": 100}")
        topicSender.send(AvroRecordData(topic, key, listOf(value, value)))
        verify(retriever, times(1))
            .getByVersion("test", false, -1)
        verify(retriever, times(1))
            .getByVersion("test", true, -1)
        val request = webServer.takeRequest()
        assertEquals("/topics/test", request.path)
        val bodyString = request.body.readString(StandardCharsets.UTF_8)
        logger.info("Reading: {}", bodyString)
        val body = READER.readTree(bodyString)
        assertEquals(10, body["key_schema_id"].asInt().toLong())
        assertEquals(10, body["value_schema_id"].asInt().toLong())
        val records = body["records"]
        assertEquals(JsonNodeType.ARRAY, records.nodeType)
        assertEquals(2, records.size().toLong())
        checkChildren(records)
    }

    @Test
    @Throws(Exception::class)
    fun resetConnection() = runTest {
        sender = sender.config {
            scope = this@runTest
        }
        var nRequests = 0
        webServer.enqueue(MockResponse().setResponseCode(500))
        assertFalse(sender.resetConnection())
        assertEquals(++nRequests, webServer.requestCount)
        var request = webServer.takeRequest()
        assertEquals("/", request.path)
        assertEquals("HEAD", request.method)
        webServer.enqueue(MockResponse())
        assertEquals(nRequests, webServer.requestCount)
        assertTrue(sender.resetConnection())
        assertEquals(++nRequests, webServer.requestCount)
        request = webServer.takeRequest()
        assertEquals("/", request.path)
        assertEquals("HEAD", request.method)
    }

    @Test
    @Throws(Exception::class)
    fun resetConnectionUnauthorized() = runTest {
        sender = sender.config {
            scope = this@runTest
        }
        webServer.enqueue(MockResponse().setResponseCode(401))
        webServer.enqueue(MockResponse().setResponseCode(401))
        try {
            sender.resetConnection()
            fail("Authentication exception expected")
        } catch (ex: AuthenticationException) {
            assertEquals(1, webServer.requestCount.toLong())
            // success
        }
        try {
            sender.resetConnection()
            fail("Authentication exception expected")
        } catch (ex: AuthenticationException) {
            assertEquals(2, webServer.requestCount.toLong())
            // success
        }
        webServer.enqueue(MockResponse().setResponseCode(200))
        try {
            assertTrue(sender.resetConnection())
            assertEquals(3, webServer.requestCount.toLong())
        } catch (ex: AuthenticationException) {
            fail("Unexpected authentication failure")
        }
    }

    @Test
    @Throws(
        IOException::class,
        InterruptedException::class,
        SchemaValidationException::class,
    )
    fun withCompression() = runTest {
        sender = sender.config {
            contentEncoding = RestKafkaSender.GZIP_CONTENT_ENCODING
        }
        webServer.enqueueJson("{\"offset\": 100}")
        val keySchema = ObservationKey.getClassSchema()
        val valueSchema = PhoneLight.getClassSchema()
        val topic = AvroTopic(
            "test",
            keySchema,
            valueSchema,
            ObservationKey::class.java,
            PhoneLight::class.java,
        )
        val topicSender = sender.sender(topic)
        val key = ObservationKey("test", "a", "b")
        val value = PhoneLight(0.1, 0.2, 0.3f)
        val keySchemaMetadata = ParsedSchemaMetadata(10, 2, keySchema)
        val valueSchemaMetadata = ParsedSchemaMetadata(10, 2, valueSchema)
        retriever.stub {
            onBlocking { getByVersion("test", false, -1) }.doReturn(keySchemaMetadata)
            onBlocking { getByVersion("test", true, -1) }.doReturn(valueSchemaMetadata)
        }
        topicSender.send(key, value)

        val request = webServer.takeRequest()
        assertEquals("gzip", request.getHeader("Content-Encoding"))
        request.body.inputStream().use { `in` ->
            GZIPInputStream(`in`).use { gzipIn ->
                val body = READER.readTree(gzipIn)
                assertEquals(10, body["key_schema_id"].asInt().toLong())
                assertEquals(10, body["value_schema_id"].asInt().toLong())
                val records = body["records"]
                assertEquals(JsonNodeType.ARRAY, records.nodeType)
                assertEquals(1, records.size().toLong())
                checkChildren(records)
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RestKafkaSenderTest::class.java)
        private val FACTORY = JsonFactory()
        private val READER = ObjectMapper(FACTORY).reader()
        private fun checkChildren(records: JsonNode) {
            for (child in records) {
                val jsonKey = child["key"]
                assertEquals(JsonNodeType.OBJECT, jsonKey.nodeType)
                assertEquals("a", jsonKey["userId"].asText())
                assertEquals("b", jsonKey["sourceId"].asText())
                val jsonValue = child["value"]
                assertEquals(JsonNodeType.OBJECT, jsonValue.nodeType)
                assertEquals(0.1, jsonValue["time"].asDouble(), 0.0)
                assertEquals(0.2, jsonValue["timeReceived"].asDouble(), 0.0)
                assertEquals(0.3f, jsonValue["light"].asDouble().toFloat(), 0f)
            }
        }

        fun MockWebServer.enqueueJson(
            body: String,
            builder: MockResponse.() -> Unit = {},
        ) = enqueue(
            MockResponse()
                .setBody(body)
                .setHeader("Content-Type", "application/json; charset=utf-8")
                .apply(builder),
        )
    }
}

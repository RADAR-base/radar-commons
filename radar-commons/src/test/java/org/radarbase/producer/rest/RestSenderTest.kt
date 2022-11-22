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
import okhttp3.Headers.Companion.headersOf
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.apache.avro.SchemaValidationException
import org.json.JSONException
import org.junit.Assert
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.mockito.Mockito
import org.radarbase.config.ServerConfig
import org.radarbase.data.AvroRecordData
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.rest.RestSender.Companion.restSender
import org.radarbase.topic.AvroTopic
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneLight
import java.io.IOException
import java.util.*
import java.util.zip.GZIPInputStream

class RestSenderTest {
    private lateinit var retriever: SchemaRetriever
    private lateinit var sender: RestSender

    @Rule
    @JvmField
    var webServer = MockWebServer()

    @Before
    fun setUp() {
        retriever = Mockito.mock(SchemaRetriever::class.java)
        sender = restSender {
            httpClient = RestClient.newRestClient {
                server = ServerConfig(webServer.url("/").toUrl())
            }
            schemaRetriever = retriever
        }
    }

    @Test
    @Throws(Exception::class)
    fun sender() {
        val keySchema = ObservationKey.getClassSchema()
        val valueSchema = PhoneLight.getClassSchema()
        val topic = AvroTopic(
            "test",
            keySchema, valueSchema, ObservationKey::class.java, PhoneLight::class.java
        )
        sender.headers = headersOf("Cookie", "ab", "Cookie", "bc")
        val topicSender = sender.sender(topic)
        val key = ObservationKey("test", "a", "b")
        val value = PhoneLight(0.1, 0.2, 0.3f)
        val keySchemaMetadata = ParsedSchemaMetadata(10, 2, keySchema)
        val valueSchemaMetadata = ParsedSchemaMetadata(10, 2, valueSchema)
        Mockito.`when`(
            retriever
                .getOrSetSchemaMetadata("test", false, keySchema, -1)
        )
            .thenReturn(keySchemaMetadata)
        Mockito.`when`(
            retriever
                .getOrSetSchemaMetadata("test", true, valueSchema, -1)
        )
            .thenReturn(valueSchemaMetadata)
        webServer.enqueue(
            MockResponse()
                .setHeader("Content-Type", "application/json; charset=utf-8")
                .setBody("{\"offset\": 100}")
        )
        topicSender.send(key, value)
        Mockito.verify(retriever, Mockito.times(1))
            .getOrSetSchemaMetadata("test", false, keySchema, -1)
        Mockito.verify(retriever, Mockito.times(1))
            .getOrSetSchemaMetadata("test", true, valueSchema, -1)
        val request = webServer.takeRequest()
        Assert.assertEquals("/topics/test", request.path)
        val body = READER.readTree(request.body.inputStream())
        Assert.assertEquals(10, body["key_schema_id"].asInt().toLong())
        Assert.assertEquals(10, body["value_schema_id"].asInt().toLong())
        val records = body["records"]
        Assert.assertEquals(JsonNodeType.ARRAY, records.nodeType)
        Assert.assertEquals(1, records.size().toLong())
        checkChildren(records)
        val receivedHeaders = request.headers
        Assert.assertEquals(listOf("ab", "bc"), receivedHeaders.values("Cookie"))
    }

    @Test
    @Throws(Exception::class)
    fun sendTwo() {
        val keySchema = ObservationKey.getClassSchema()
        val valueSchema = PhoneLight.getClassSchema()
        val topic = AvroTopic(
            "test",
            keySchema, valueSchema, ObservationKey::class.java, PhoneLight::class.java
        )
        val topicSender = sender.sender(topic)
        val key = ObservationKey("test", "a", "b")
        val value = PhoneLight(0.1, 0.2, 0.3f)
        val keySchemaMetadata = ParsedSchemaMetadata(10, 2, keySchema)
        val valueSchemaMetadata = ParsedSchemaMetadata(10, 2, valueSchema)
        Mockito.`when`(
            retriever
                .getOrSetSchemaMetadata("test", false, keySchema, -1)
        )
            .thenReturn(keySchemaMetadata)
        Mockito.`when`(
            retriever
                .getOrSetSchemaMetadata("test", true, valueSchema, -1)
        )
            .thenReturn(valueSchemaMetadata)
        webServer.enqueue(
            MockResponse()
                .setHeader("Content-Type", "application/json; charset=utf-8")
                .setBody("{\"offset\": 100}")
        )
        topicSender.send(AvroRecordData(topic, key, Arrays.asList(value, value)))
        Mockito.verify(retriever, Mockito.times(1))
            .getOrSetSchemaMetadata("test", false, keySchema, -1)
        Mockito.verify(retriever, Mockito.times(1))
            .getOrSetSchemaMetadata("test", true, valueSchema, -1)
        val request = webServer.takeRequest()
        Assert.assertEquals("/topics/test", request.path)
        val body = READER.readTree(request.body.inputStream())
        Assert.assertEquals(10, body["key_schema_id"].asInt().toLong())
        Assert.assertEquals(10, body["value_schema_id"].asInt().toLong())
        val records = body["records"]
        Assert.assertEquals(JsonNodeType.ARRAY, records.nodeType)
        Assert.assertEquals(2, records.size().toLong())
        checkChildren(records)
    }

    @Test
    @Throws(Exception::class)
    fun resetConnection() {
        var nRequests = 0
        webServer.enqueue(MockResponse().setResponseCode(500))
        Assert.assertFalse(sender.isConnected)
        Assert.assertEquals(++nRequests, webServer.requestCount)
        var request = webServer.takeRequest()
        Assert.assertEquals("/", request.path)
        Assert.assertEquals("HEAD", request.method)
        webServer.enqueue(MockResponse().setResponseCode(500))
        Assert.assertFalse(sender.resetConnection())
        Assert.assertEquals(++nRequests, webServer.requestCount)
        request = webServer.takeRequest()
        Assert.assertEquals("/", request.path)
        Assert.assertEquals("HEAD", request.method)
        webServer.enqueue(MockResponse())
        Assert.assertFalse(sender.isConnected)
        Assert.assertEquals(nRequests, webServer.requestCount)
        Assert.assertTrue(sender.resetConnection())
        Assert.assertEquals(++nRequests, webServer.requestCount)
        request = webServer.takeRequest()
        Assert.assertEquals("/", request.path)
        Assert.assertEquals("HEAD", request.method)
    }

    @Test
    @Throws(Exception::class)
    fun resetConnectionUnauthorized() {
        webServer.enqueue(MockResponse().setResponseCode(401))
        try {
            sender.isConnected
            Assert.fail("Authentication exception expected")
        } catch (ex: AuthenticationException) {
            // success
        }
        try {
            sender.isConnected
            Assert.fail("Authentication exception expected")
        } catch (ex: AuthenticationException) {
            // success
        }
        webServer.enqueue(MockResponse().setResponseCode(401))
        try {
            sender.resetConnection()
            Assert.fail("Authentication exception expected")
        } catch (ex: AuthenticationException) {
            Assert.assertEquals(2, webServer.requestCount.toLong())
            // success
        }
        webServer.enqueue(MockResponse().setResponseCode(200))
        try {
            Assert.assertTrue(sender.resetConnection())
        } catch (ex: AuthenticationException) {
            Assert.assertEquals(3, webServer.requestCount.toLong())
            Assert.fail("Unexpected authentication failure")
        }
    }

    @Test
    @Throws(
        IOException::class,
        InterruptedException::class,
        SchemaValidationException::class,
        JSONException::class
    )
    fun withCompression() {
        sender.setCompression(true)
        webServer.enqueue(
            MockResponse()
                .setHeader("Content-Type", "application/json; charset=utf-8")
                .setBody("{\"offset\": 100}")
        )
        val keySchema = ObservationKey.getClassSchema()
        val valueSchema = PhoneLight.getClassSchema()
        val topic = AvroTopic(
            "test",
            keySchema, valueSchema, ObservationKey::class.java, PhoneLight::class.java
        )
        val topicSender = sender.sender(topic)
        val key = ObservationKey("test", "a", "b")
        val value = PhoneLight(0.1, 0.2, 0.3f)
        val keySchemaMetadata = ParsedSchemaMetadata(10, 2, keySchema)
        val valueSchemaMetadata = ParsedSchemaMetadata(10, 2, valueSchema)
        Mockito.`when`(
            retriever
                .getOrSetSchemaMetadata("test", false, keySchema, -1)
        )
            .thenReturn(keySchemaMetadata)
        Mockito.`when`(
            retriever
                .getOrSetSchemaMetadata("test", true, valueSchema, -1)
        )
            .thenReturn(valueSchemaMetadata)
        topicSender.send(key, value)
        val request = webServer.takeRequest()
        Assert.assertEquals("gzip", request.getHeader("Content-Encoding"))
        request.body.inputStream().use { `in` ->
            GZIPInputStream(`in`).use { gzipIn ->
                val body = READER.readTree(gzipIn)
                Assert.assertEquals(10, body["key_schema_id"].asInt().toLong())
                Assert.assertEquals(10, body["value_schema_id"].asInt().toLong())
                val records = body["records"]
                Assert.assertEquals(JsonNodeType.ARRAY, records.nodeType)
                Assert.assertEquals(1, records.size().toLong())
                checkChildren(records)
            }
        }
    }

    companion object {
        private val FACTORY = JsonFactory()
        private val READER = ObjectMapper(FACTORY).reader()
        private fun checkChildren(records: JsonNode) {
            for (child in records) {
                val jsonKey = child["key"]
                Assert.assertEquals(JsonNodeType.OBJECT, jsonKey.nodeType)
                Assert.assertEquals("a", jsonKey["userId"].asText())
                Assert.assertEquals("b", jsonKey["sourceId"].asText())
                val jsonValue = child["value"]
                Assert.assertEquals(JsonNodeType.OBJECT, jsonValue.nodeType)
                Assert.assertEquals(0.1, jsonValue["time"].asDouble(), 0.0)
                Assert.assertEquals(0.2, jsonValue["timeReceived"].asDouble(), 0.0)
                Assert.assertEquals(0.3f, jsonValue["light"].asDouble().toFloat(), 0f)
            }
        }
    }
}

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
package org.radarbase.producer.schema

import io.ktor.client.plugins.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.apache.avro.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.radarbase.producer.io.timeout
import org.radarbase.producer.rest.RestKafkaSenderTest.Companion.enqueueJson
import org.radarbase.producer.schema.SchemaRetriever.Companion.schemaRetriever
import org.radarbase.producer.schema.SchemaRetriever.Companion.subject
import java.io.IOException
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class SchemaRetrieverTest {
    private lateinit var mockServer: MockWebServer
    private lateinit var retriever: SchemaRetriever

    @Suppress("HttpUrlsUsage")
    @BeforeEach
    fun setUp() {
        mockServer = MockWebServer().apply {
            start()
        }
        retriever = schemaRetriever("http://${mockServer.hostName}:${mockServer.port}/base/") {
            httpClient {
                defaultRequest {
                    url("http://${mockServer.hostName}:${mockServer.port}/base/")
                }
                timeout(1.seconds)
            }
        }
    }

    @AfterEach
    @Throws(IOException::class)
    fun tearDown() {
        mockServer.close()
    }

    @Test
    fun subject() {
        assertEquals("bla-value", subject("bla", true))
        assertEquals("bla-key", subject("bla", false))
    }

    // Already queried schema is cached and does not need another request
    @Test
    fun testSchemaMetadata() = runTest {
        // Not yet queried schema needs a new request, so if the server does not respond, an
        // IOException is thrown.
        mockServer.enqueueJson("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}")
        val (id, version, schema) = retriever.getByVersion("bla", true, 2)
        assertEquals(10, id)
        assertEquals(2, version)
        assertEquals(Schema.create(Schema.Type.STRING), schema)
        assertEquals("/base/subjects/bla-value/versions/2", mockServer.takeRequest().path)

        // Already queried schema is cached and does not need another request
        val (id1, version1, schema1) = retriever.getByVersion("bla", true, 2)
        assertEquals(10, id1)
        assertEquals(2, version1)
        assertEquals(Schema.create(Schema.Type.STRING), schema1)
        assertEquals(1, mockServer.requestCount.toLong())

        // Not yet queried schema needs a new request, so if the server does not respond, an
        // IOException is thrown.
        mockServer.enqueue(MockResponse().setResponseCode(500))
        assertThrows<IOException> {
            retriever.getByVersion(
                "bla",
                false,
                2
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun addSchemaMetadata() = runTest {
        mockServer.enqueueJson("{\"id\":10}")
        var id = retriever.addSchema("bla", true, Schema.create(Schema.Type.STRING))
        assertEquals(10, id.toLong())
        assertEquals(1, mockServer.requestCount.toLong())
        var request = mockServer.takeRequest()
        assertEquals("{\"schema\":\"\\\"string\\\"\"}", request.body.readUtf8())
        val schemaFields = listOf(
            Schema.Field("a", Schema.create(Schema.Type.INT), "that a", 10)
        )
        val record = Schema.createRecord("C", "that C", "org.radarcns", false, schemaFields)
        mockServer.enqueueJson("{\"id\":11}")
        id = retriever.addSchema("bla", true, record)
        assertEquals(11, id.toLong())
        request = mockServer.takeRequest()
        assertEquals(
            "{\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"C\\\",\\\"namespace\\\":\\\"org.radarcns\\\",\\\"doc\\\":\\\"that C\\\",\\\"fields\\\":[{\\\"name\\\":\\\"a\\\",\\\"type\\\":\\\"int\\\",\\\"doc\\\":\\\"that a\\\",\\\"default\\\":10}]}\"}",
            request.body.readUtf8()
        )
    }
}

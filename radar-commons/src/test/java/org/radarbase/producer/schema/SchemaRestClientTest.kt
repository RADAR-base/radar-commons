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

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockWebServer
import org.apache.avro.Schema
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.radarbase.producer.io.timeout
import org.radarbase.producer.rest.RestKafkaSenderTest.Companion.enqueueJson
import java.io.IOException
import java.time.Duration
import java.util.*

@OptIn(ExperimentalCoroutinesApi::class)
class SchemaRestClientTest {
    private lateinit var mockServer: MockWebServer
    private lateinit var retriever: SchemaRestClient
    @BeforeEach
    fun setUp() {
        mockServer = MockWebServer()
        retriever = SchemaRestClient(
            HttpClient(CIO) {
                timeout(Duration.ofSeconds(1))
            },
            baseUrl = "http://${mockServer.hostName}:${mockServer.port}/base/"
        )
    }

    @AfterEach
    @Throws(IOException::class)
    fun tearDown() {
        mockServer.close()
    }

    @Test
    @Throws(Exception::class)
    fun retrieveSchemaMetadata() = runTest {
        mockServer.enqueueJson("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}")
        val (id, version, schema) = retriever.retrieveSchemaMetadata("bla-value", -1)
        assertEquals(10, id)
        assertEquals(2, version)
        assertEquals(Schema.create(Schema.Type.STRING), schema)
        assertEquals("/base/subjects/bla-value/versions/latest", mockServer.takeRequest().path)
    }

    @Test
    @Throws(Exception::class)
    fun retrieveSchemaMetadataVersion() = runTest {
        mockServer.enqueueJson("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}")
        val (id, version, schema) = retriever.retrieveSchemaMetadata("bla-value", 2)
        assertEquals(10, id)
        assertEquals(2, version)
        assertEquals(Schema.create(Schema.Type.STRING), schema)
        assertEquals("/base/subjects/bla-value/versions/2", mockServer.takeRequest().path)
    }
}

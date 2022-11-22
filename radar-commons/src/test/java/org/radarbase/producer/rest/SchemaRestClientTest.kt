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

import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.apache.avro.Schema
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.radarbase.config.ServerConfig
import java.io.IOException
import java.util.*
import java.util.concurrent.TimeUnit

class SchemaRestClientTest {
    private lateinit var mockServer: MockWebServer
    private lateinit var retriever: SchemaRestClient
    @Before
    fun setUp() {
        mockServer = MockWebServer()
        retriever = SchemaRestClient(
            RestClient.globalRestClient {
                server = ServerConfig("http://${mockServer.hostName}:${mockServer.port}/base")
                timeout(1L, TimeUnit.SECONDS)
            }
        )
    }

    @After
    @Throws(IOException::class)
    fun tearDown() {
        mockServer.close()
    }

    @Test
    @Throws(Exception::class)
    fun retrieveSchemaMetadata() {
        mockServer.enqueue(MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"))
        val (id, version, schema) = retriever.retrieveSchemaMetadata("bla-value", -1)
        assertEquals(10, id)
        assertEquals(2, version)
        assertEquals(Schema.create(Schema.Type.STRING), schema)
        assertEquals("/base/subjects/bla-value/versions/latest", mockServer.takeRequest().path)
    }

    @Test
    @Throws(Exception::class)
    fun retrieveSchemaMetadataVersion() {
        mockServer.enqueue(MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"))
        val (id, version, schema) = retriever.retrieveSchemaMetadata("bla-value", 2)
        assertEquals(10, id)
        assertEquals(2, version)
        assertEquals(Schema.create(Schema.Type.STRING), schema)
        assertEquals("/base/subjects/bla-value/versions/2", mockServer.takeRequest().path)
    }
}

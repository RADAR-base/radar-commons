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

import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.Request
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.radarbase.config.ServerConfig
import java.net.URL
import java.util.concurrent.TimeUnit

class RestClientTest {
    private lateinit var server: MockWebServer
    private lateinit var config: ServerConfig
    private lateinit var client: RestClient
    @Before
    fun setUp() {
        server = MockWebServer()
        config = ServerConfig(server.url("base").toUrl())
        client = RestClient.newRestClient {
            server = config
            timeout(1, TimeUnit.SECONDS)
        }
    }

    @Test
    @Throws(Exception::class)
    fun request() {
        server.enqueue(MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"))
        val request = client.buildRequest("myPath")
        client.request(request).use { response ->
            Assert.assertTrue(response.isSuccessful)
            Assert.assertEquals(
                "{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}",
                response.body!!.string()
            )
        }
        val recordedRequest = server.takeRequest()
        Assert.assertEquals("GET", recordedRequest.method)
        Assert.assertEquals("/base/myPath", recordedRequest.path)
    }

    @Test
    @Throws(Exception::class)
    fun requestStringPath() {
        server.enqueue(MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"))
        client.request("myPath").use { response ->
            Assert.assertTrue(response.isSuccessful)
            Assert.assertEquals(
                "{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}",
                response.body!!.string()
            )
        }
        val recordedRequest = server.takeRequest()
        Assert.assertEquals("GET", recordedRequest.method)
        Assert.assertEquals("/base/myPath", recordedRequest.path)
    }

    @Test
    @Throws(Exception::class)
    fun requestString() {
        server.enqueue(MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"))
        val response = client.requestString(client.buildRequest("myPath"))
        Assert.assertEquals("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}", response)
        val recordedRequest = server.takeRequest()
        Assert.assertEquals("GET", recordedRequest.method)
        Assert.assertEquals("/base/myPath", recordedRequest.path)
    }

    @Test(expected = RestException::class)
    @Throws(Exception::class)
    fun requestStringEmpty() {
        server.enqueue(MockResponse().setResponseCode(500))
        client.requestString(client.buildRequest("myPath"))
    }

    @Test
    @Throws(Exception::class)
    fun requestBuilder() {
        val request: Request = client.buildRequest("myPath")
        Assert.assertEquals(request.url, URL(config.url, "myPath").toHttpUrlOrNull())
    }

    @Throws(Exception::class)
    @Test
    fun testRelativeUrl() {
        val url = client.relativeUrl("myPath")
        Assert.assertEquals(server.hostName, url.host)
        Assert.assertEquals(server.port.toLong(), url.port.toLong())
        Assert.assertEquals("http", url.scheme)
        Assert.assertEquals("/base/myPath", url.encodedPath)
    }
}

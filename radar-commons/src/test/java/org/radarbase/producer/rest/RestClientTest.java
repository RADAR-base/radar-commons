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

package org.radarbase.producer.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Before;
import org.junit.Test;
import org.radarbase.config.ServerConfig;

public class RestClientTest {
    private MockWebServer server;
    private ServerConfig config;
    private RestClient client;

    @Before
    public void setUp() {
        server = new MockWebServer();
        config = new ServerConfig(server.url("base").url());
        client = RestClient.newClient()
                .server(config)
                .timeout(1, TimeUnit.SECONDS)
                .build();
    }

    @Test
    public void request() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"));
        Request request = client.requestBuilder("myPath").build();
        try (Response response = client.request(request)) {
            assertTrue(response.isSuccessful());
            assertEquals("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}", response.body().string());
        }
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals("GET", recordedRequest.getMethod());
        assertEquals("/base/myPath", recordedRequest.getPath());
    }

    @Test
    public void requestStringPath() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"));
        try (Response response = client.request("myPath")) {
            assertTrue(response.isSuccessful());
            assertEquals("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}", response.body().string());
        }
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals("GET", recordedRequest.getMethod());
        assertEquals("/base/myPath", recordedRequest.getPath());
    }

    @Test
    public void requestString() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"));
        String response = client.requestString(client.requestBuilder("myPath").build());
        assertEquals("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}", response);
        RecordedRequest recordedRequest = server.takeRequest();
        assertEquals("GET", recordedRequest.getMethod());
        assertEquals("/base/myPath", recordedRequest.getPath());
    }

    @Test(expected = RestException.class)
    public void requestStringEmpty() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(500));
        client.requestString(client.requestBuilder("myPath").build());
    }

    @Test
    public void requestBuilder() throws Exception {
        Request.Builder builder = client.requestBuilder("myPath");
        Request request = builder.build();
        assertEquals(request.url(), HttpUrl.get(new URL(config.getUrl(), "myPath")));
    }

    @Test
    public void getRelativeUrl() throws Exception {
        HttpUrl url = client.getRelativeUrl("myPath");
        assertEquals(server.getHostName(), url.host());
        assertEquals(server.getPort(), url.port());
        assertEquals("http", url.scheme());
        assertEquals("/base/myPath", url.encodedPath());
    }
}
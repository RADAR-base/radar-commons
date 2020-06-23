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
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.radarbase.config.ServerConfig;

public class SchemaRestClientTest {
    private MockWebServer server;
    private SchemaRestClient retriever;

    @Before
    public void setUp() {
        server = new MockWebServer();
        ServerConfig config = new ServerConfig();
        config.setProtocol("http");
        config.setHost(server.getHostName());
        config.setPort(server.getPort());
        config.setPath("base");
        retriever = new SchemaRestClient(RestClient.global()
                .server(Objects.requireNonNull(config))
                .timeout(1L, TimeUnit.SECONDS)
                .build());
    }

    @After
    public void tearDown() throws IOException {
        server.close();
    }

    @Test
    public void retrieveSchemaMetadata() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"));
        ParsedSchemaMetadata metadata = retriever.retrieveSchemaMetadata("bla-value", -1);
        assertEquals(Integer.valueOf(10), metadata.getId());
        assertEquals(Integer.valueOf(2), metadata.getVersion());
        assertEquals(Schema.create(Schema.Type.STRING), metadata.getSchema());
        assertEquals("/base/subjects/bla-value/versions/latest", server.takeRequest().getPath());
    }


    @Test
    public void retrieveSchemaMetadataVersion() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"));
        ParsedSchemaMetadata metadata = retriever.retrieveSchemaMetadata("bla-value", 2);
        assertEquals(Integer.valueOf(10), metadata.getId());
        assertEquals(Integer.valueOf(2), metadata.getVersion());
        assertEquals(Schema.create(Schema.Type.STRING), metadata.getSchema());
        assertEquals("/base/subjects/bla-value/versions/2", server.takeRequest().getPath());
    }
}

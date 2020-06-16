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
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.radarbase.config.ServerConfig;

public class SchemaRetrieverTest {
    private MockWebServer server;
    private SchemaRetriever retriever;

    @Before
    public void setUp() {
        server = new MockWebServer();
        ServerConfig config = new ServerConfig();
        config.setProtocol("http");
        config.setHost(server.getHostName());
        config.setPort(server.getPort());
        config.setPath("base");
        retriever = new SchemaRetriever(config, 1L);
    }

    @After
    public void tearDown() throws IOException {
        server.close();
    }

    @Test
    public void subject() {
        assertEquals("bla-value", SchemaRetriever.subject("bla", true));
        assertEquals("bla-key", SchemaRetriever.subject("bla", false));
    }

    @Test
    public void getSchemaMetadata() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"));
        ParsedSchemaMetadata metadata = retriever.getSchemaMetadata("bla", true, 2);
        assertEquals(Integer.valueOf(10), metadata.getId());
        assertEquals(Integer.valueOf(2), metadata.getVersion());
        assertEquals(Schema.create(Schema.Type.STRING), metadata.getSchema());
        assertEquals("/base/subjects/bla-value/versions/2", server.takeRequest().getPath());

        // Already queried schema is cached and does not need another request
        ParsedSchemaMetadata metadata2 = retriever.getSchemaMetadata("bla", true, 2);
        assertEquals(Integer.valueOf(10), metadata2.getId());
        assertEquals(Integer.valueOf(2), metadata2.getVersion());
        assertEquals(Schema.create(Schema.Type.STRING), metadata2.getSchema());
        assertEquals(1, server.getRequestCount());

        // Not yet queried schema needs a new request, so if the server does not respond, an
        // IOException is thrown.
        server.enqueue(new MockResponse().setResponseCode(500));
        assertThrows(IOException.class, () -> retriever.getSchemaMetadata("bla", false, 2));
    }

    @Test
    public void addSchemaMetadata() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"id\":10}"));
        int id = retriever.addSchema("bla", true, Schema.create(Schema.Type.STRING));
        assertEquals(10, id);

        assertEquals(1, server.getRequestCount());
        RecordedRequest request = server.takeRequest();
        assertEquals("{\"schema\":\"\\\"string\\\"\"}", request.getBody().readUtf8());

        List<Field> schemaFields = Collections.singletonList(
                new Field("a", Schema.create(Schema.Type.INT), "that a", 10));

        Schema record = Schema.createRecord("C", "that C", "org.radarcns", false, schemaFields);
        server.enqueue(new MockResponse().setBody("{\"id\":11}"));
        id = retriever.addSchema("bla", true, record);
        assertEquals(11, id);
        request = server.takeRequest();
        assertEquals("{\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"C\\\",\\\"namespace\\\":\\\"org.radarcns\\\",\\\"doc\\\":\\\"that C\\\",\\\"fields\\\":[{\\\"name\\\":\\\"a\\\",\\\"type\\\":\\\"int\\\",\\\"doc\\\":\\\"that a\\\",\\\"default\\\":10}]}\"}", request.getBody().readUtf8());
    }

    @Test
    public void getOrSetSchemaMetadataSet() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(404));
        server.enqueue(new MockResponse().setBody("{\"id\":10}"));
        server.enqueue(new MockResponse().setBody("{\"id\":10, \"version\": 2}"));
        ParsedSchemaMetadata metadata = retriever.getOrSetSchemaMetadata("bla", true, Schema.create(Schema.Type.STRING), -1);
        assertEquals(Integer.valueOf(10), metadata.getId());
        assertEquals(Schema.create(Schema.Type.STRING), metadata.getSchema());

        assertEquals(3, server.getRequestCount());
        server.takeRequest();
        RecordedRequest request = server.takeRequest();
        assertEquals("{\"schema\":\"\\\"string\\\"\"}", request.getBody().readUtf8());
        assertEquals("/base/subjects/bla-value/versions", request.getPath());

        metadata = retriever.getOrSetSchemaMetadata("bla", true, Schema.create(Schema.Type.STRING), -1);
        assertEquals(Integer.valueOf(10), metadata.getId());
        assertEquals(Schema.create(Schema.Type.STRING), metadata.getSchema());
    }

    @Test
    public void getOrSetSchemaMetadataGet() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"id\":10,\"version\":2,\"schema\":\"\\\"string\\\"\"}"));
        ParsedSchemaMetadata metadata = retriever.getOrSetSchemaMetadata("bla", true, Schema.create(Schema.Type.STRING), 2);
        assertEquals(Integer.valueOf(10), metadata.getId());
        assertEquals(Integer.valueOf(2), metadata.getVersion());
        assertEquals(Schema.create(Schema.Type.STRING), metadata.getSchema());

        assertEquals(1, server.getRequestCount());
        RecordedRequest request = server.takeRequest();
        assertEquals("/base/subjects/bla-value/versions/2", request.getPath());

        metadata = retriever.getOrSetSchemaMetadata("bla", true, Schema.create(Schema.Type.STRING), 2);
        assertEquals(Integer.valueOf(10), metadata.getId());
        assertEquals(Schema.create(Schema.Type.STRING), metadata.getSchema());
    }
}

/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.producer.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.radarcns.config.ServerConfig;
import org.radarcns.data.SpecificRecordEncoder;
import org.radarcns.key.MeasurementKey;
import org.radarcns.producer.SchemaRetriever;

/**
 * Created by joris on 20/03/2017.
 */
public class RestSenderTest {

    private SchemaRetriever retriever;
    private RestSender<MeasurementKey, MeasurementKey> sender;
    private ServerConfig config;

    @Rule
    public MockWebServer webServer = new MockWebServer();

    @Before
    public void setUp() {
        this.retriever = mock(SchemaRetriever.class);
        SpecificRecordEncoder encoder = new SpecificRecordEncoder(false);

        config = new ServerConfig();
        config.setProtocol("http");
        config.setHost(webServer.getHostName());
        config.setPort(webServer.getPort());
        config.setPath("");
        this.sender = new RestSender<>(config, retriever, encoder, encoder, 10);
    }

    @Test
    public void sender() throws Exception {
    }

    @Test
    public void resetConnection() throws Exception {

    }

    @Test
    public void isConnected() throws Exception {
        webServer.enqueue(new MockResponse());
        assertTrue(sender.isConnected());
        RecordedRequest request = webServer.takeRequest();
        assertEquals("/", request.getPath());
        assertEquals("HEAD", request.getMethod());
    }

    @Test
    public void close() throws Exception {

    }

}
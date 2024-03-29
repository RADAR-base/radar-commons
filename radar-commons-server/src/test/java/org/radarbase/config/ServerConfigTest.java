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

package org.radarbase.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.net.URL;
import org.junit.jupiter.api.Test;

/**
 * Created by joris on 01/05/2017.
 */
public class ServerConfigTest {

    @Test
    public void getUrl() throws Exception {
        ServerConfig config = new ServerConfig("http://something.else/that");
        URL url = config.getUrl();
        assertEquals("something.else", url.getAuthority());
        assertEquals("http", url.getProtocol());
        assertEquals("/that/", url.getFile());
        assertEquals(-1, url.getPort());
        assertEquals(80, url.getDefaultPort());
        assertEquals("http://something.else/that/", url.toExternalForm());
        assertEquals("http://something.else/that/", config.toString());
    }

    @Test
    public void jacksonUrl() throws IOException {
        ObjectReader reader = new ObjectMapper(new YAMLFactory()).readerFor(ServerConfig.class);
        assertEquals("http://52.210.59.174/schema/",
                ((ServerConfig)reader.readValue(
                        "protocol: http\n"
                                + "host: 52.210.59.174\n"
                                + "path: /schema/"))
                        .getUrlString());
        assertEquals("http://52.210.59.174/schema/",
                ((ServerConfig)reader.readValue(
                        "protocol: http\n"
                        + "host: 52.210.59.174\n"
                        + "path: /schema"))
                        .getUrlString());
    }
}

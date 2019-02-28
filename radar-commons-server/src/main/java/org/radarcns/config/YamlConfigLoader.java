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

package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A YAML Config file loader, to load YAML files into equivalent POJO Objects.
 */
public class YamlConfigLoader {
    public static final JsonFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(YAML_FACTORY);

    static {
        YAML_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        // only serialize fields, not getters, etc.
        YAML_MAPPER.setVisibility(YAML_MAPPER.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
        YAML_MAPPER.addMixIn(ServerConfig.class, ServerConfigMixin.class);
    }

    /**
     * Load a YAML file into given class.
     * @param file file to load.
     * @param configClass class for the config.
     * @param <T> type of the config.
     * @return loaded file.
     * @throws IOException if the file cannot be opened or parsed.
     */
    public <T> T load(Path file, Class<T> configClass) throws IOException {
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            return YAML_MAPPER.readValue(reader, configClass);
        }
    }

    /** Store config into given YAML file. */
    public void store(Path file, Object config) throws IOException {
        try (Writer writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            YAML_MAPPER.writeValue(writer, config);
        }
    }

    /**
     * Pretty-print the given object as a YAML string.
     */
    public String prettyString(Object config) {
        ObjectMapper prettyPrintMapper = YAML_MAPPER.copy();
        // pretty print
        prettyPrintMapper.enable(SerializationFeature.INDENT_OUTPUT);
        // make ConfigRadar the root element
        prettyPrintMapper.enable(SerializationFeature.WRAP_ROOT_VALUE);

        try {
            return prettyPrintMapper.writeValueAsString(config);
        } catch (JsonProcessingException ex) {
            throw new UnsupportedOperationException("Cannot serialize config", ex);
        }
    }
}

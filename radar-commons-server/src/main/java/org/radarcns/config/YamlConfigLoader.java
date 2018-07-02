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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;

/**
 * A YAML Config file loader, to load YAML files into equivalent POJO Objects.
 */
public class YamlConfigLoader {

    private final ObjectMapper mapper;

    /**
     * Default loader.
     */
    public YamlConfigLoader() {
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        // only serialize fields, not getters, etc.
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
        mapper.addMixIn(ServerConfig.class, ServerConfigMixin.class);
    }

    /**
     * Load a YAML file into given class.
     * @param file file to load.
     * @param configClass class for the config.
     * @param <T> type of the config.
     * @return loaded file.
     * @throws IOException if the file cannot be opened or parsed.
     */
    public <T> T load(File file, Class<T> configClass) throws IOException {
        return mapper.readValue(file, configClass);
    }

    /** Store config into given YAML file. */
    public void store(File file, Object config) throws IOException {
        mapper.writeValue(file, config);
    }

    /**
     * Pretty-print the given object as a YAML string.
     */
    public String prettyString(Object config) {
        // pretty print
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        // make ConfigRadar the root element
        mapper.enable(SerializationFeature.WRAP_ROOT_VALUE);

        try {
            return mapper.writeValueAsString(config);
        } catch (JsonProcessingException ex) {
            throw new UnsupportedOperationException("Cannot serialize config", ex);
        } finally {
            mapper.disable(SerializationFeature.INDENT_OUTPUT);
            mapper.disable(SerializationFeature.WRAP_ROOT_VALUE);
        }
    }
}

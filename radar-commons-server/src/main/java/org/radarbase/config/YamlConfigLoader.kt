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
package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import java.io.IOException
import java.nio.file.Path
import kotlin.io.path.bufferedReader
import kotlin.io.path.bufferedWriter

/**
 * A YAML Config file loader, to load YAML files into equivalent POJO Objects.
 */
class YamlConfigLoader @JvmOverloads constructor(mapperConfigurator: (ObjectMapper) -> Unit = {}) {
    private val yamlMapper = ObjectMapper(YAML_FACTORY).apply {
        setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
        setVisibility(
            serializationConfig.getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE),
        )

        addMixIn(ServerConfig::class.java, ServerConfigMixin::class.java)
    }

    /** Config loader that does some additional object mapper configuration after initialization.  */
    init {
        mapperConfigurator(yamlMapper)
    }

    /**
     * Load a YAML file into given class.
     * @param file file to load.
     * @param configClass class for the config.
     * @param <T> type of the config.
     * @return loaded file.
     * @throws IOException if the file cannot be opened or parsed.
     </T> */
    @Throws(IOException::class)
    fun <T> load(file: Path, configClass: Class<T>): T = file.bufferedReader()
        .use { reader ->
            yamlMapper.readValue(reader, configClass)
        }

    /** Store config into given YAML file.  */
    @Throws(IOException::class)
    fun store(file: Path, config: Any) {
        file.bufferedWriter().use { writer ->
            yamlMapper.writeValue(writer, config)
        }
    }

    /**
     * Pretty-print the given object as a YAML string.
     */
    fun prettyString(config: Any): String = yamlMapper.copy().run {
        // pretty print
        enable(SerializationFeature.INDENT_OUTPUT)
        // make ConfigRadar the root element
        enable(SerializationFeature.WRAP_ROOT_VALUE)
        return try {
            writeValueAsString(config)
        } catch (ex: JsonProcessingException) {
            throw UnsupportedOperationException("Cannot serialize config", ex)
        }
    }

    companion object {
        val YAML_FACTORY: JsonFactory = YAMLFactory()
    }
}

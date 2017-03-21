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

package org.radarcns.util.serde;

import java.util.Map;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.config.ConfigException;
import org.radarcns.config.ServerConfig;
import org.radarcns.producer.SchemaRetriever;

/**
 * Abstract class for KafkaAvro(De)serializer
 */
public class AbstractKafkaAvroSerde {
    public static final String SCHEMA_REGISTRY_CONFIG = "schema.registry";

    protected boolean ofValue;
    protected final EncoderFactory encoderFactory = EncoderFactory.get();
    protected SchemaRetriever schemaRetriever;

    public AbstractKafkaAvroSerde() {
        // Bean constructor
    }

    public AbstractKafkaAvroSerde(SchemaRetriever retriever) {
        this.schemaRetriever = retriever;
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.ofValue = !isKey;
        if (schemaRetriever != null) {
            return;
        }
        Object schemaConfig = configs.get(SCHEMA_REGISTRY_CONFIG);
        if (schemaConfig instanceof SchemaRetriever) {
            schemaRetriever = (SchemaRetriever) schemaConfig;
        } else if (schemaConfig instanceof ServerConfig) {
            schemaRetriever = new SchemaRetriever((ServerConfig) schemaConfig, 30);
        } else {
            throw new ConfigException("Config " + SCHEMA_REGISTRY_CONFIG
                    + " is not correctly configured. "
                    + "Pass a " + SchemaRetriever.class + " or a " + ServerConfig.class);
        }
    }

    public void close() {
        // noop
    }
}

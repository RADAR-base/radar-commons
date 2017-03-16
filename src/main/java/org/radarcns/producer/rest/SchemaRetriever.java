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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import org.apache.avro.Schema;
import org.radarcns.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retriever of an Avro Schema */
public class SchemaRetriever {
    private static final Logger logger = LoggerFactory.getLogger(SchemaRetriever.class);
    private static final MediaType type = MediaType.parse("application/vnd.schemaregistry.v1+json; charset=utf-8");
    private final ConcurrentMap<String, ParsedSchemaMetadata> cache;
    private final ObjectReader reader;
    private final JsonFactory jsonFactory;
    private RestClient httpClient;

    public SchemaRetriever(ServerConfig config, long connectionTimeout) {
        Objects.requireNonNull(config);
        cache = new ConcurrentHashMap<>();
        jsonFactory = new JsonFactory();
        reader = new ObjectMapper(jsonFactory).readerFor(JsonNode.class);
        httpClient = new RestClient(config, connectionTimeout);
    }

    public synchronized void setConnectionTimeout(long connectionTimeout) {
        if (httpClient.getTimeout() != connectionTimeout) {
            httpClient = new RestClient(httpClient.getConfig(), connectionTimeout);
        }
    }

    /** The subject in the Avro Schema Registry, given a Kafka topic. */
    protected static String subject(String topic, boolean ofValue) {
        return topic + (ofValue ? "-value" : "-key");
    }

    /** Retrieve schema metadata */
    protected ParsedSchemaMetadata retrieveSchemaMetadata(String subject) throws IOException {
        Request request = httpClient.requestBuilder("/subjects/" + subject + "/versions/latest")
                .addHeader("Accept", "application/json")
                .get()
                .build();

        try (Response response = httpClient.request(request)) {
            if (!response.isSuccessful()) {
                throw new IOException("Cannot retrieve metadata (HTTP " + response.code()
                        + ": " + response.message() + ") -> " + response.body().string());
            }
            JsonNode node = reader.readTree(response.body().byteStream());
            int version = node.get("version").asInt();
            int schemaId = node.get("id").asInt();
            Schema schema = parseSchema(node.get("schema").asText());
            return new ParsedSchemaMetadata(schemaId, version, schema);
        }
    }

    public ParsedSchemaMetadata getSchemaMetadata(String topic, boolean ofValue) throws IOException {
        String subject = subject(topic, ofValue);
        ParsedSchemaMetadata value = cache.get(subject);
        if (value == null) {
            value = retrieveSchemaMetadata(subject);
            ParsedSchemaMetadata oldValue = cache.putIfAbsent(subject, value);
            if (oldValue != null) {
                value = oldValue;
            }
        }
        return value;
    }

    /** Parse a schema from string. */
    protected Schema parseSchema(String schemaString) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    /**
     * Add schema metadata to the retriever.
     *
     * This implementation only adds it to the cache.
     */
    public void addSchemaMetadata(String topic, boolean ofValue, ParsedSchemaMetadata metadata) throws IOException {
        String subject = subject(topic, ofValue);
        if (metadata.getId() == null) {

            Request request = httpClient.requestBuilder("/subjects/" + subject + "/versions")
                    .addHeader("Accept", "application/json")
                    .post(new SchemaRequestBody(metadata.getSchema()))
                    .build();

            try (Response response = httpClient.request(request)) {
                if (!response.isSuccessful()) {
                    throw new IOException("Cannot post schema (HTTP " + response.code()
                            + ": " + response.message() + ") -> " + response.body().string());
                }
                JsonNode node = reader.readTree(response.body().byteStream());
                int schemaId = node.get("id").asInt();
                metadata.setId(schemaId);
            }
        }
        cache.put(subject, metadata);
    }

    /**
     * Get schema metadata, and if none is found, add a new schema.
     */
    public ParsedSchemaMetadata getOrSetSchemaMetadata(String topic, boolean ofValue, Schema schema) throws IOException {
        ParsedSchemaMetadata metadata;
        try {
            metadata = getSchemaMetadata(topic, ofValue);
            if (metadata.getSchema().equals(schema)) {
                return metadata;
            }
        } catch (IOException ex) {
            logger.warn("Schema for {} value was not yet added to the schema registry.", topic);
        }

        metadata = new ParsedSchemaMetadata(null, null, schema);
        addSchemaMetadata(topic, ofValue, metadata);
        return metadata;
    }

    private class SchemaRequestBody extends RequestBody {
        private final Schema schema;

        private SchemaRequestBody(Schema schema) {
            this.schema = schema;
        }

        @Override
        public MediaType contentType() {
            return type;
        }

        @Override
        public void writeTo(BufferedSink sink) throws IOException {
            try (OutputStream out = sink.outputStream();
                    JsonGenerator writer = jsonFactory.createGenerator(out, JsonEncoding.UTF8)) {
                writer.writeStartObject();
                writer.writeFieldName("schema");
                writer.writeString(schema.toString());
                writer.writeEndObject();
            }
        }
    }
}

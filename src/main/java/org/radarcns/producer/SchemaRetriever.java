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

package org.radarcns.producer;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.radarcns.config.ServerConfig;
import org.radarcns.producer.rest.ParsedSchemaMetadata;
import org.radarcns.producer.rest.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retriever of an Avro Schema */
public class SchemaRetriever implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(SchemaRetriever.class);
    private static final MediaType CONTENT_TYPE = MediaType.parse(
            "application/vnd.schemaregistry.v1+json; charset=utf-8");
    private static final Schema NULL_SCHEMA = Schema.create(Type.NULL);
    private static final Map<Class<?>, Schema> PRIMITIVE_SCHEMAS = new HashMap<>();

    static {
        PRIMITIVE_SCHEMAS.put(Long.class, Schema.create(Type.LONG));
        PRIMITIVE_SCHEMAS.put(Integer.class, Schema.create(Type.INT));
        PRIMITIVE_SCHEMAS.put(Float.class, Schema.create(Type.FLOAT));
        PRIMITIVE_SCHEMAS.put(Double.class, Schema.create(Type.DOUBLE));
        PRIMITIVE_SCHEMAS.put(String.class, Schema.create(Type.STRING));
        PRIMITIVE_SCHEMAS.put(Boolean.class, Schema.create(Type.BOOLEAN));
        PRIMITIVE_SCHEMAS.put(byte[].class, Schema.create(Type.BYTES));
    }

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
            httpClient.close();
            httpClient = new RestClient(httpClient.getConfig(), connectionTimeout);
        }
    }

    private synchronized RestClient getRestClient() {
        return httpClient;
    }

    /** The subject in the Avro Schema Registry, given a Kafka topic. */
    protected static String subject(String topic, boolean ofValue) {
        return topic + (ofValue ? "-value" : "-key");
    }

    /** Retrieve schema metadata */
    protected ParsedSchemaMetadata retrieveSchemaMetadata(String subject, int version)
            throws IOException {
        String path = "/subjects/" + subject + "/versions/";
        if (version > 0) {
            path += version;
        } else {
            path += "latest";
        }
        RestClient restClient = getRestClient();
        Request request = restClient.requestBuilder(path)
                .addHeader("Accept", "application/json")
                .get()
                .build();

        try (Response response = restClient.request(request)) {
            if (!response.isSuccessful()) {
                throw new IOException("Cannot retrieve metadata (HTTP " + response.code()
                        + ": " + response.message() + ") -> " + response.body().string());
            }
            JsonNode node = reader.readTree(response.body().byteStream());
            int newVersion = version < 1 ? node.get("version").asInt() : version;
            int schemaId = node.get("id").asInt();
            Schema schema = parseSchema(node.get("schema").asText());
            return new ParsedSchemaMetadata(schemaId, newVersion, schema);
        }
    }

    public ParsedSchemaMetadata getSchemaMetadata(String topic, boolean ofValue, int version)
            throws IOException {
        String subject = subject(topic, ofValue);
        ParsedSchemaMetadata value = cache.get(subject);
        if (value == null) {
            value = retrieveSchemaMetadata(subject, version);
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
    public void addSchemaMetadata(String topic, boolean ofValue, ParsedSchemaMetadata metadata)
            throws IOException {
        String subject = subject(topic, ofValue);
        if (metadata.getId() == null) {
            RestClient restClient = getRestClient();

            Request request = restClient.requestBuilder("/subjects/" + subject + "/versions")
                    .addHeader("Accept", "application/json")
                    .post(new SchemaRequestBody(metadata.getSchema()))
                    .build();

            try (Response response = restClient.request(request)) {
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
     *
     * @param version version to get or 0 if the latest version can be used.
     */
    public ParsedSchemaMetadata getOrSetSchemaMetadata(String topic, boolean ofValue, Schema schema,
            int version) throws IOException {
        ParsedSchemaMetadata metadata;
        try {
            metadata = getSchemaMetadata(topic, ofValue, version);
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

    @Override
    public void close() {
        getRestClient().close();
    }

    private class SchemaRequestBody extends RequestBody {
        private final Schema schema;

        private SchemaRequestBody(Schema schema) {
            this.schema = schema;
        }

        @Override
        public MediaType contentType() {
            return CONTENT_TYPE;
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

    /**
     * Get the schema of a generic object. This supports null, primitive types, String, and
     * {@link org.apache.avro.generic.GenericContainer}.
     * @param object object of recognized CONTENT_TYPE
     * @throws IllegalArgumentException if passed object is not a recognized CONTENT_TYPE
     */
    public static Schema getSchema(Object object) {
        if (object == null) {
            return NULL_SCHEMA;
        }
        Schema schema = PRIMITIVE_SCHEMAS.get(object.getClass());
        if (schema != null) {
            return schema;
        }
        if (object instanceof GenericContainer) {
            return ((GenericContainer)object).getSchema();
        }
        throw new IllegalArgumentException("Passed object " + object + " of class "
                + object.getClass() + " can not be schematized. "
                + "Pass null, a primitive CONTENT_TYPE or a GenericContainer.");
    }
}

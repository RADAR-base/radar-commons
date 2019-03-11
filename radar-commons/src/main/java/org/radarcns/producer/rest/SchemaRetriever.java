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

package org.radarcns.producer.rest;

import static org.radarcns.util.Strings.utf8;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.BufferedSink;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.json.JSONException;
import org.json.JSONObject;
import org.radarcns.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retriever of an Avro Schema. Internally, only {@link JSONObject} is used to manage JSON data,
 * to keep the class as lean as possible.
 */
public class SchemaRetriever {
    private static final Logger logger = LoggerFactory.getLogger(SchemaRetriever.class);
    private static final MediaType CONTENT_TYPE = MediaType.parse(
            "application/vnd.schemaregistry.v1+json; charset=utf-8");
    private static final Schema NULL_SCHEMA = Schema.create(Type.NULL);
    private static final Map<Class<?>, Schema> PRIMITIVE_SCHEMAS = new HashMap<>();
    private static final byte[] SCHEMA = utf8("{\"schema\":");
    private static final long MAX_VALIDITY = 86400L;

    static {
        PRIMITIVE_SCHEMAS.put(Long.class, Schema.create(Type.LONG));
        PRIMITIVE_SCHEMAS.put(Integer.class, Schema.create(Type.INT));
        PRIMITIVE_SCHEMAS.put(Float.class, Schema.create(Type.FLOAT));
        PRIMITIVE_SCHEMAS.put(Double.class, Schema.create(Type.DOUBLE));
        PRIMITIVE_SCHEMAS.put(String.class, Schema.create(Type.STRING));
        PRIMITIVE_SCHEMAS.put(Boolean.class, Schema.create(Type.BOOLEAN));
        PRIMITIVE_SCHEMAS.put(byte[].class, Schema.create(Type.BYTES));
    }

    private final ConcurrentMap<String, TimedSchemaMetadata> cache;
    private RestClient httpClient;

    /**
     * Schema retriever for a Confluent Schema Registry.
     * @param config schema registry configuration.
     * @param connectionTimeout timeout in seconds.
     */
    public SchemaRetriever(ServerConfig config, long connectionTimeout) {
        cache = new ConcurrentHashMap<>();
        httpClient = RestClient.global()
                .server(Objects.requireNonNull(config))
                .timeout(connectionTimeout, TimeUnit.SECONDS)
                .build();
    }

    /**
     * Set the connection timeout.
     * @param connectionTimeout timeout in seconds.
     */
    public synchronized void setConnectionTimeout(long connectionTimeout) {
        if (httpClient.getTimeout() != connectionTimeout) {
            httpClient = httpClient.newBuilder()
                    .timeout(connectionTimeout, TimeUnit.SECONDS)
                    .build();
        }
    }

    private synchronized RestClient getRestClient() {
        return httpClient;
    }

    /** The subject in the Avro Schema Registry, given a Kafka topic. */
    protected static String subject(String topic, boolean ofValue) {
        return topic + (ofValue ? "-value" : "-key");
    }

    /** Retrieve schema metadata from server. */
    protected ParsedSchemaMetadata retrieveSchemaMetadata(String subject, int version)
            throws JSONException, IOException {
        StringBuilder pathBuilder = new StringBuilder(50)
                .append("/subjects/")
                .append(subject)
                .append("/versions/");
        if (version > 0) {
            pathBuilder.append(version);
        } else {
            pathBuilder.append("latest");
        }
        RestClient restClient = getRestClient();
        Request request = restClient.requestBuilder(pathBuilder.toString())
                .addHeader("Accept", "application/json")
                .build();

        String response = restClient.requestString(request);
        JSONObject node = new JSONObject(response);
        int newVersion = version < 1 ? node.getInt("version") : version;
        int schemaId = node.getInt("id");
        Schema schema = parseSchema(node.getString("schema"));
        return new ParsedSchemaMetadata(schemaId, newVersion, schema);
    }

    /** Get schema metadata. Cached schema metadata will be used if present. */
    public ParsedSchemaMetadata getSchemaMetadata(String topic, boolean ofValue, int version)
            throws JSONException, IOException {
        String subject = subject(topic, ofValue);
        TimedSchemaMetadata value = cache.get(subject);
        if (value == null || value.isExpired()) {
            value = new TimedSchemaMetadata(retrieveSchemaMetadata(subject, version));
            cache.put(subject, value);
        }
        return value.metadata;
    }

    /** Parse a schema from string. */
    protected Schema parseSchema(String schemaString) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    /**
     * Add schema metadata to the retriever. This implementation only adds it to the cache.
     */
    public void addSchemaMetadata(String topic, boolean ofValue, ParsedSchemaMetadata metadata)
            throws JSONException, IOException {
        String subject = subject(topic, ofValue);
        if (metadata.getId() == null) {
            RestClient restClient = getRestClient();

            Request request = restClient.requestBuilder("/subjects/" + subject + "/versions")
                    .addHeader("Accept", "application/json")
                    .post(new SchemaRequestBody(metadata.getSchema()))
                    .build();

            String response = restClient.requestString(request);
            JSONObject node = new JSONObject(response);
            int schemaId = node.getInt("id");
            metadata.setId(schemaId);
        }
        cache.put(subject, new TimedSchemaMetadata(metadata));
    }

    /**
     * Get schema metadata, and if none is found, add a new schema.
     *
     * @param version version to get or 0 if the latest version can be used.
     */
    public ParsedSchemaMetadata getOrSetSchemaMetadata(String topic, boolean ofValue, Schema schema,
            int version) throws JSONException, IOException {
        try {
            return getSchemaMetadata(topic, ofValue, version);
        } catch (IOException ex) {
            logger.warn("Schema for {} value was not yet added to the schema registry.", topic);
            ParsedSchemaMetadata metadata = new ParsedSchemaMetadata(null, null, schema);
            addSchemaMetadata(topic, ofValue, metadata);
            return metadata;
        }
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
            sink.write(SCHEMA);
            sink.writeUtf8(JSONObject.quote(schema.toString()));
            sink.writeByte('}');
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

    private class TimedSchemaMetadata {
        private final ParsedSchemaMetadata metadata;
        private final long expiry;

        TimedSchemaMetadata(ParsedSchemaMetadata metadata) {
            expiry = System.currentTimeMillis() + MAX_VALIDITY * 1000L;
            this.metadata = Objects.requireNonNull(metadata);
        }

        boolean isExpired() {
            return expiry < System.currentTimeMillis();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return metadata.equals(((TimedSchemaMetadata)o).metadata);
        }

        @Override
        public int hashCode() {
            return metadata.hashCode();
        }
    }
}

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericContainer;
import org.json.JSONException;
import org.json.JSONObject;
import org.radarbase.config.ServerConfig;
import org.radarbase.util.TimedInt;
import org.radarbase.util.TimedValue;
import org.radarbase.util.TimedVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retriever of an Avro Schema. Internally, only {@link JSONObject} is used to manage JSON data,
 * to keep the class as lean as possible.
 */
public class SchemaRetriever {
    private static final Logger logger = LoggerFactory.getLogger(SchemaRetriever.class);
    private static final Schema NULL_SCHEMA = Schema.create(Type.NULL);
    private static final Map<Class<?>, Schema> PRIMITIVE_SCHEMAS = new HashMap<>();
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

    private final ConcurrentMap<Integer, TimedValue<Schema>> idCache =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Schema, TimedInt> schemaCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Integer, TimedInt>> subjectVersionCache =
            new ConcurrentHashMap<>();

    private final SchemaRestClient restClient;
    private final long cacheValidity;

    public SchemaRetriever(RestClient client, long cacheValidity) {
        restClient = new SchemaRestClient(client);
        this.cacheValidity = cacheValidity;
    }

    public SchemaRetriever(RestClient client) {
        this(client, MAX_VALIDITY);
    }

    /**
     * Schema retriever for a Confluent Schema Registry.
     * @param config schema registry configuration.
     * @param connectionTimeout timeout in seconds.
     */
    public SchemaRetriever(ServerConfig config, long connectionTimeout) {
        this(RestClient.global()
                        .server(Objects.requireNonNull(config))
                        .timeout(connectionTimeout, TimeUnit.SECONDS)
                        .build());
    }

    /**
     * Schema retriever for a Confluent Schema Registry.
     * @param config schema registry configuration.
     * @param connectionTimeout timeout in seconds.
     * @param cacheValidity timeout in seconds for considering a schema stale.
     */
    public SchemaRetriever(ServerConfig config, long connectionTimeout, long cacheValidity) {
        this(RestClient.global()
                .server(Objects.requireNonNull(config))
                .timeout(connectionTimeout, TimeUnit.SECONDS)
                .build(), cacheValidity);
    }

    /**
     * Add schema metadata to the retriever. This implementation only adds it to the cache.
     * @return schema ID
     */
    public int addSchema(String topic, boolean ofValue, Schema schema)
            throws JSONException, IOException {
        String subject = subject(topic, ofValue);
        int id = restClient.addSchema(subject, schema);
        cache(new ParsedSchemaMetadata(id, null, schema), subject, false);
        return id;
    }

    /**
     * Get schema metadata, and if none is found, add a new schema.
     *
     * @param version version to get or 0 if the latest version can be used.
     */
    public ParsedSchemaMetadata getOrSetSchemaMetadata(String topic, boolean ofValue, Schema schema,
            int version) throws JSONException, IOException {
        try {
            return getBySubjectAndVersion(topic, ofValue, version);
        } catch (IOException ex) {
            logger.warn("Schema for {} value was not yet added to the schema registry.", topic);
            addSchema(topic, ofValue, schema);
            return getMetadata(topic, ofValue, schema, version <= 0);
        }
    }

    /** Get a schema by its ID. */
    public Schema getById(int id) throws IOException {
        TimedValue<Schema> value = idCache.get(id);
        if (value == null || value.isExpired()) {
            value = new TimedValue<>(restClient.retrieveSchemaById(id), cacheValidity);
            idCache.put(id, value);
            schemaCache.put(value.value, new TimedInt(id, cacheValidity));
        }
        return value.value;
    }

    /** Gets a schema by ID and check that it is present in the given topic. */
    public ParsedSchemaMetadata getBySubjectAndId(String topic, boolean ofValue, int id)
            throws IOException {
        Schema schema = getById(id);
        String subject = subject(topic, ofValue);
        ParsedSchemaMetadata metadata = getCachedVersion(subject, id, null, schema);
        return metadata != null ? metadata : getMetadata(topic, ofValue, schema);
    }

    /** Get schema metadata. Cached schema metadata will be used if present. */
    public ParsedSchemaMetadata getBySubjectAndVersion(String topic, boolean ofValue, int version)
            throws JSONException, IOException {
        String subject = subject(topic, ofValue);
        ConcurrentMap<Integer, TimedInt> versionMap = computeIfAbsent(subjectVersionCache, subject,
                new ConcurrentHashMap<>());
        TimedInt id = versionMap.get(Math.max(version, 0));
        if (id == null || id.isExpired()) {
            ParsedSchemaMetadata metadata = restClient.retrieveSchemaMetadata(subject, version);
            cache(metadata, subject, version <= 0);
            return metadata;
        } else {
            Schema schema = getById(id.value);
            ParsedSchemaMetadata metadata = getCachedVersion(subject, id.value, version, schema);

            return metadata != null ? metadata : getMetadata(topic, ofValue, schema, version <= 0);
        }
    }

    /** Get all schema versions in a subject. */
    public ParsedSchemaMetadata getMetadata(String topic, boolean ofValue, Schema schema)
            throws IOException {
        return getMetadata(topic, ofValue, schema, false);
    }


    /** Get the metadata of a specific schema in a topic. */
    public ParsedSchemaMetadata getMetadata(String topic, boolean ofValue, Schema schema,
            boolean ofLatestVersion) throws IOException {
        TimedInt id = schemaCache.get(schema);
        String subject = subject(topic, ofValue);

        if (id != null && !id.isExpired()) {
            ParsedSchemaMetadata metadata = getCachedVersion(subject, id.value, null, schema);
            if (metadata != null) {
                return metadata;
            }
        }

        ParsedSchemaMetadata metadata = restClient.requestMetadata(subject, schema);
        cache(metadata, subject, ofLatestVersion);
        return metadata;
    }


    /**
     * Get cached metadata.
     * @param subject schema registry subject
     * @param id schema ID.
     * @param reportedVersion version requested by the client. Null if no version was requested. This version will be used if the actual version was not cached.
     * @param schema schema to use.
     * @return metadata if present. Returns null if no metadata is cached or if no version is cached and the reportedVersion is null.
     */
    protected ParsedSchemaMetadata getCachedVersion(String subject, int id,
            Integer reportedVersion, Schema schema) {
        Integer version = reportedVersion;
        if (version == null || version <= 0) {
            ConcurrentMap<Integer, TimedInt> versions = subjectVersionCache.get(subject);
            if (versions != null) {
                for (Map.Entry<Integer, TimedInt> entry : versions.entrySet()) {
                    if (!entry.getValue().isExpired() && entry.getKey() != 0
                            && entry.getValue().value == id) {
                        version = entry.getKey();
                        break;
                    }
                }
            }
            if (version == null || version < 1) {
                return null;
            }
        }
        return new ParsedSchemaMetadata(id, version, schema);
    }

    protected void cache(ParsedSchemaMetadata metadata, String subject, boolean latest) {
        TimedInt id = new TimedInt(metadata.getId(), cacheValidity);
        schemaCache.put(metadata.getSchema(), id);
        if (subject != null && metadata.getVersion() != null) {
            ConcurrentMap<Integer, TimedInt> versionCache = computeIfAbsent(subjectVersionCache,
                    subject, new ConcurrentHashMap<>());

            versionCache.put(metadata.getVersion(), id);
            if (latest) {
                versionCache.put(0, id);
            }
        }
        idCache.put(metadata.getId(), new TimedValue<>(metadata.getSchema(), cacheValidity));
    }

    /**
     * Remove expired entries from cache.
     */
    public void pruneCache() {
        prune(schemaCache);
        prune(idCache);
        for (ConcurrentMap<Integer, TimedInt> versionMap : subjectVersionCache.values()) {
            prune(versionMap);
        }
    }

    private void prune(Map<?, ? extends TimedVariable> map) {
        for (Entry<?, ? extends TimedVariable> entry : map.entrySet()) {
            if (entry.getValue().isExpired()) {
                map.remove(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Remove all entries from cache.
     */
    public void clearCache() {
        subjectVersionCache.clear();
        idCache.clear();
        schemaCache.clear();
    }

    /** The subject in the Avro Schema Registry, given a Kafka topic. */
    protected static String subject(String topic, boolean ofValue) {
        return topic + (ofValue ? "-value" : "-key");
    }

    private static <K, V> V computeIfAbsent(ConcurrentMap<K, V> original, K key, V newValue) {
        V existingValue = original.putIfAbsent(key, newValue);
        return existingValue != null ? existingValue : newValue;
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

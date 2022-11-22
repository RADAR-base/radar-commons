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
package org.radarbase.producer.rest

import org.apache.avro.Schema
import org.json.JSONException
import org.radarbase.util.CacheConfig
import org.radarbase.util.TimedInt
import org.radarbase.util.TimedValue
import org.radarbase.util.TimedVariable.Companion.prune
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * Retriever of an Avro Schema.
 */
open class SchemaRetriever @JvmOverloads constructor(
    client: RestClient,
    private val cacheConfig: CacheConfig = CacheConfig(MAX_VALIDITY),
) {
    private val idCache: ConcurrentMap<Int, TimedValue<Schema>> = ConcurrentHashMap()
    private val schemaCache: ConcurrentMap<Schema, TimedInt> = ConcurrentHashMap()
    private val subjectVersionCache: ConcurrentMap<String, ConcurrentMap<Int, TimedInt>> =
        ConcurrentHashMap()
    private val restClient: SchemaRestClient = SchemaRestClient(client)

    /**
     * Add schema metadata to the retriever. This implementation only adds it to the cache.
     * @return schema ID
     */
    @Throws(JSONException::class, IOException::class)
    fun addSchema(topic: String, ofValue: Boolean, schema: Schema): Int {
        val subject = subject(topic, ofValue)
        val id = restClient.addSchema(subject, schema)
        cache(ParsedSchemaMetadata(id, null, schema), subject, false)
        return id
    }

    /**
     * Get schema metadata, and if none is found, add a new schema.
     *
     * @param version version to get or 0 if the latest version can be used.
     */
    @Throws(JSONException::class, IOException::class)
    open fun getOrSetSchemaMetadata(
        topic: String, ofValue: Boolean, schema: Schema,
        version: Int
    ): ParsedSchemaMetadata {
        return try {
            getBySubjectAndVersion(topic, ofValue, version)
        } catch (ex: RestException) {
            if (ex.statusCode == 404) {
                logger.warn("Schema for {} value was not yet added to the schema registry.", topic)
                addSchema(topic, ofValue, schema)
                getMetadata(topic, ofValue, schema, version <= 0)
            } else {
                throw ex
            }
        }
    }

    /** Get a schema by its ID.  */
    @Throws(IOException::class)
    fun getById(id: Int): Schema {
        var value = idCache[id]
        if (value == null || value.isExpired) {
            value = TimedValue(restClient.retrieveSchemaById(id), cacheConfig)
            idCache[id] = value
            schemaCache[value.value] = TimedInt(id, cacheConfig)
        }
        return value.value
    }

    /** Gets a schema by ID and check that it is present in the given topic.  */
    @Throws(IOException::class)
    fun getBySubjectAndId(topic: String, ofValue: Boolean, id: Int): ParsedSchemaMetadata {
        val schema = getById(id)
        val subject = subject(topic, ofValue)
        val metadata = getCachedVersion(subject, id, null, schema)
        return metadata ?: getMetadata(topic, ofValue, schema)
    }

    /** Get schema metadata. Cached schema metadata will be used if present.  */
    @Throws(JSONException::class, IOException::class)
    fun getBySubjectAndVersion(
        topic: String,
        ofValue: Boolean,
        version: Int
    ): ParsedSchemaMetadata {
        val subject = subject(topic, ofValue)
        val versionMap = subjectVersionCache.computeIfAbsent(
            subject,
            ::ConcurrentHashMap
        )
        val id = versionMap[version.coerceAtLeast(0)]
        return if (id == null || id.isExpired) {
            val metadata = restClient.retrieveSchemaMetadata(subject, version)
            cache(metadata, subject, version <= 0)
            metadata
        } else {
            val schema = getById(id.value)
            val metadata = getCachedVersion(subject, id.value, version, schema)
            metadata ?: getMetadata(topic, ofValue, schema, version <= 0)
        }
    }

    /** Get all schema versions in a subject.  */
    @Throws(IOException::class)
    fun getMetadata(topic: String, ofValue: Boolean, schema: Schema): ParsedSchemaMetadata {
        return getMetadata(topic, ofValue, schema, false)
    }

    /** Get the metadata of a specific schema in a topic.  */
    @Throws(IOException::class)
    fun getMetadata(
        topic: String, ofValue: Boolean, schema: Schema,
        ofLatestVersion: Boolean
    ): ParsedSchemaMetadata {
        val id = schemaCache[schema]
        val subject = subject(topic, ofValue)
        if (id != null && !id.isExpired) {
            val metadata = getCachedVersion(subject, id.value, null, schema)
            if (metadata != null) {
                return metadata
            }
        }
        val metadata = restClient.requestMetadata(subject, schema)
        cache(metadata, subject, ofLatestVersion)
        return metadata
    }

    /**
     * Get cached metadata.
     * @param subject schema registry subject
     * @param id schema ID.
     * @param reportedVersion version requested by the client. Null if no version was requested.
     * This version will be used if the actual version was not cached.
     * @param schema schema to use.
     * @return metadata if present. Returns null if no metadata is cached or if no version is cached
     * and the reportedVersion is null.
     */
    protected fun getCachedVersion(
        subject: String, id: Int,
        reportedVersion: Int?, schema: Schema?
    ): ParsedSchemaMetadata? {
        var version = reportedVersion
        if (version == null || version <= 0) {
            version = findCachedVersion(id, subjectVersionCache[subject])
            if (version == null || version <= 0) {
                return null
            }
        }
        return ParsedSchemaMetadata(id, version, schema!!)
    }

    private fun findCachedVersion(id: Int, cache: ConcurrentMap<Int, TimedInt>?): Int? {
        cache ?: return null
        return cache.entries.find { (k, v) -> !v.isExpired && k != 0 && v.value == id }
            ?.key
    }

    protected fun cache(metadata: ParsedSchemaMetadata, subject: String, latest: Boolean) {
        val id = TimedInt(metadata.id, cacheConfig)
        schemaCache[metadata.schema] = id
        if (metadata.version != null) {
            val versionCache = subjectVersionCache.computeIfAbsent(
                subject,
                ::ConcurrentHashMap
            )
            versionCache[metadata.version] = id
            if (latest) {
                versionCache[0] = id
            }
        }
        idCache[metadata.id] = TimedValue(metadata.schema, cacheConfig)
    }

    /**
     * Remove expired entries from cache.
     */
    fun pruneCache() {
        schemaCache.prune()
        idCache.prune()
        for (versionMap in subjectVersionCache.values) {
            versionMap.prune()
        }
    }

    /**
     * Remove all entries from cache.
     */
    fun clearCache() {
        subjectVersionCache.clear()
        idCache.clear()
        schemaCache.clear()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SchemaRetriever::class.java)
        private val MAX_VALIDITY = Duration.ofDays(1)

        /** The subject in the Avro Schema Registry, given a Kafka topic.  */
        @JvmStatic
        fun subject(topic: String, ofValue: Boolean): String = if (ofValue) "$topic-value" else "$topic-key"

        private fun <K, V> MutableMap<K, V>.computeIfAbsent(
            key: K,
            newValueGenerator: () -> V,
        ): V {
            return get(key)
                ?: run {
                    val newValue = newValueGenerator()
                    putIfAbsent(key, newValue)
                        ?: newValue
                }
        }
    }
}

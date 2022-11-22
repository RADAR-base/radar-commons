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
package org.radarbase.producer.schema

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.Dispatchers
import org.apache.avro.Schema
import org.radarbase.producer.rest.RestException
import org.radarbase.util.RadarProducerDsl
import org.radarbase.util.TimedInt
import org.radarbase.util.TimedValue
import org.radarbase.util.TimedVariable.Companion.prune
import org.radarbase.util.TimeoutConfig
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.Objects.hash
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.coroutines.CoroutineContext

/**
 * Retriever of an Avro Schema.
 */
open class SchemaRetriever(config: Config) {
    private val idCache: ConcurrentMap<Int, TimedValue<Schema>> = ConcurrentHashMap()
    private val schemaCache: ConcurrentMap<Schema, TimedInt> = ConcurrentHashMap()
    private val subjectVersionCache: ConcurrentMap<String, ConcurrentMap<Int, TimedInt>> =
        ConcurrentHashMap()

    private val baseUrl = config.baseUrl
    private val ioContext = config.ioContext
    private val httpClient = requireNotNull(config.httpClient) { "Missing HTTP client" }

    private val restClient: SchemaRestClient = SchemaRestClient(httpClient, baseUrl, ioContext)
    private val schemaTimeout = config.schemaTimeout

    /**
     * Add schema metadata to the retriever. This implementation only adds it to the cache.
     * @return schema ID
     */
    @Throws(IOException::class)
    suspend fun addSchema(topic: String, ofValue: Boolean, schema: Schema): Int {
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
    @Throws(IOException::class)
    open suspend fun getOrSet(
        topic: String,
        ofValue: Boolean,
        schema: Schema,
        version: Int
    ): ParsedSchemaMetadata {
        return try {
            getByVersion(topic, ofValue, version)
        } catch (ex: RestException) {
            if (ex.status == HttpStatusCode.NotFound) {
                logger.warn("Schema for {} value was not yet added to the schema registry.", topic)
                addSchema(topic, ofValue, schema)
                metadata(topic, ofValue, schema, version <= 0)
            } else {
                throw ex
            }
        }
    }

    /** Get a schema by its ID.  */
    @Throws(IOException::class)
    suspend fun getById(id: Int): Schema {
        var value = idCache[id]
        if (value == null || value.isExpired) {
            value = TimedValue(restClient.retrieveSchemaById(id), schemaTimeout)
            idCache[id] = value
            schemaCache[value.value] = TimedInt(id, schemaTimeout)
        }
        return value.value
    }

    /** Gets a schema by ID and check that it is present in the given topic.  */
    @Throws(IOException::class)
    suspend fun getById(topic: String, ofValue: Boolean, id: Int): ParsedSchemaMetadata {
        val schema = getById(id)
        val subject = subject(topic, ofValue)
        return cached(subject, id, null, schema)
            ?: metadata(topic, ofValue, schema)
    }

    /** Get schema metadata. Cached schema metadata will be used if present.  */
    @Throws(IOException::class)
    suspend fun getByVersion(
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
            val metadata = cached(subject, id.value, version, schema)
            metadata ?: metadata(topic, ofValue, schema, version <= 0)
        }
    }

    /** Get all schema versions in a subject.  */
    @Throws(IOException::class)
    open suspend fun metadata(
        topic: String,
        ofValue: Boolean,
        schema: Schema
    ): ParsedSchemaMetadata = metadata(topic, ofValue, schema, false)

    /** Get the metadata of a specific schema in a topic.  */
    @Throws(IOException::class)
    suspend fun metadata(
        topic: String,
        ofValue: Boolean,
        schema: Schema,
        ofLatestVersion: Boolean
    ): ParsedSchemaMetadata {
        val id = schemaCache[schema]
        val subject = subject(topic, ofValue)
        if (id != null && !id.isExpired) {
            val metadata = cached(subject, id.value, null, schema)
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
    protected fun cached(
        subject: String,
        id: Int,
        reportedVersion: Int?,
        schema: Schema?
    ): ParsedSchemaMetadata? {
        var version = reportedVersion
        if (version == null || version <= 0) {
            version = subjectVersionCache[subject]?.find(id)
            if (version == null || version <= 0) {
                return null
            }
        }
        return ParsedSchemaMetadata(id, version, schema!!)
    }

    private fun ConcurrentMap<Int, TimedInt>.find(id: Int): Int? = entries
        .find { (k, v) -> !v.isExpired && k != 0 && v.value == id }
        ?.key

    protected fun cache(metadata: ParsedSchemaMetadata, subject: String, latest: Boolean) {
        val id = TimedInt(metadata.id, schemaTimeout)
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
        idCache[metadata.id] = TimedValue(metadata.schema, schemaTimeout)
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

    @RadarProducerDsl
    class Config(
        val baseUrl: String,
    ) {
        var httpClient: HttpClient? = null
        var schemaTimeout: TimeoutConfig = DEFAULT_SCHEMA_TIMEOUT_CONFIG
        var ioContext: CoroutineContext = Dispatchers.IO
        fun httpClient(config: HttpClientConfig<*>.() -> Unit) {
            httpClient = httpClient?.config(config)
                ?: HttpClient(CIO)
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Config

            return baseUrl == other.baseUrl &&
                httpClient == other.httpClient &&
                schemaTimeout == other.schemaTimeout &&
                ioContext == other.ioContext
        }

        override fun hashCode(): Int = hash(baseUrl, httpClient, schemaTimeout, ioContext)
    }

    fun config(config: Config.() -> Unit): SchemaRetriever {
        val currentConfig = toConfig()
        val newConfig = toConfig().apply(config)
        return if (currentConfig != newConfig) {
            SchemaRetriever(newConfig)
        } else this
    }

    private fun toConfig(): Config = Config(baseUrl = baseUrl).apply {
        httpClient = this@SchemaRetriever.httpClient
        schemaTimeout = this@SchemaRetriever.schemaTimeout
        ioContext = this@SchemaRetriever.ioContext
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SchemaRetriever::class.java)
        private val DEFAULT_SCHEMA_TIMEOUT_CONFIG = TimeoutConfig(Duration.ofDays(1))

        fun schemaRetriever(baseUrl: String, config: Config.() -> Unit): SchemaRetriever {
            return SchemaRetriever(Config(baseUrl).apply(config))
        }

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

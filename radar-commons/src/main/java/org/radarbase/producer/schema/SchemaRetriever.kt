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

import io.ktor.client.HttpClient
import io.ktor.client.HttpClientConfig
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.apache.avro.Schema
import org.radarbase.kotlin.coroutines.CacheConfig
import org.radarbase.kotlin.coroutines.CachedValue
import org.radarbase.util.RadarProducerDsl
import java.io.IOException
import java.lang.ref.SoftReference
import java.util.Objects.hash
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes

typealias VersionCache = ConcurrentMap<Int, CachedValue<ParsedSchemaMetadata>>

/**
 * Retriever of an Avro Schema.
 */
open class SchemaRetriever(config: Config) {
    private val idCache: ConcurrentMap<Int, SoftReference<Schema>> = ConcurrentHashMap()
    private val schemaCache: ConcurrentMap<Schema, CachedValue<ParsedSchemaMetadata>> = ConcurrentHashMap()
    private val subjectVersionCache: ConcurrentMap<String, VersionCache> = ConcurrentHashMap()

    private val baseUrl = config.baseUrl
    private val ioContext = config.ioContext
    private val httpClient = requireNotNull(config.httpClient) { "Missing HTTP client" }

    val restClient: SchemaRestClient = SchemaRestClient(httpClient, baseUrl, ioContext)

    private val schemaTimeout = config.schemaTimeout

    /**
     * Add schema metadata to the retriever. This implementation only adds it to the cache.
     * @return schema ID
     */
    @Throws(IOException::class)
    suspend fun addSchema(topic: String, ofValue: Boolean, schema: Schema): Int = coroutineScope {
        val subject = subject(topic, ofValue)
        val metadata = restClient.addSchema(subject, schema)

        if (metadata.version != null) {
            launch {
                cachedMetadata(subject, metadata.schema).set(metadata)
            }
            launch {
                cachedVersion(subject, metadata.version).set(metadata)
            }
        }
        metadata.id
    }

    /** Get schema metadata. Cached schema metadata will be used if present.  */
    @Throws(IOException::class)
    open suspend fun getByVersion(
        topic: String,
        ofValue: Boolean,
        version: Int,
    ): ParsedSchemaMetadata {
        val subject = subject(topic, ofValue)
        val versionMap = subjectVersionCache.computeIfAbsent(
            subject,
            ::ConcurrentHashMap,
        )
        val metadata = versionMap.cachedVersion(subject, version).get()
        if (version <= 0 && metadata.version != null) {
            versionMap.cachedVersion(subject, metadata.version).set(metadata)
        }
        return metadata
    }

    /** Get schema metadata. Cached schema metadata will be used if present.  */
    @Throws(IOException::class)
    open suspend fun getById(
        topic: String,
        ofValue: Boolean,
        id: Int,
    ): ParsedSchemaMetadata {
        val subject = subject(topic, ofValue)
        val schema = idCache[id]?.get()
            ?: restClient.retrieveSchemaById(id)

        return cachedMetadata(subject, schema).get()
    }

    /** Get the metadata of a specific schema in a topic.  */
    @Throws(IOException::class)
    open suspend fun metadata(
        topic: String,
        ofValue: Boolean,
        schema: Schema,
    ): ParsedSchemaMetadata {
        val subject = subject(topic, ofValue)
        return cachedMetadata(subject, schema).get()
    }

    private fun cachedMetadata(
        subject: String,
        schema: Schema,
    ): CachedValue<ParsedSchemaMetadata> = schemaCache.computeIfAbsent(schema) {
        CachedValue(schemaTimeout) {
            val metadata = restClient.requestMetadata(subject, schema)
            if (metadata.version != null) {
                cachedVersion(subject, metadata.version).set(metadata)
            }
            idCache[metadata.id] = SoftReference(metadata.schema)
            metadata
        }
    }

    private suspend fun cachedVersion(
        subject: String,
        version: Int,
    ): CachedValue<ParsedSchemaMetadata> = subjectVersionCache
        .computeIfAbsent(
            subject,
            ::ConcurrentHashMap,
        )
        .cachedVersion(subject, version)

    private suspend fun VersionCache.cachedVersion(
        subject: String,
        version: Int,
    ): CachedValue<ParsedSchemaMetadata> {
        val useVersion = version.coerceAtLeast(0)
        val versionId = computeIfAbsent(useVersion) {
            CachedValue(schemaTimeout) {
                val metadata = restClient.retrieveSchemaMetadata(subject, version)
                cachedMetadata(subject, metadata.schema).set(metadata)
                idCache[metadata.id] = SoftReference(metadata.schema)
                metadata
            }
        }
        return versionId
    }

    private suspend fun <T> MutableCollection<CachedValue<T>>.prune() {
        val iter = iterator()
        while (iter.hasNext()) {
            val staleValue = iter.next().getFromCache()
                ?: continue

            if (
                staleValue is CachedValue.CacheError ||
                (
                    staleValue is CachedValue.CacheValue<T> &&
                        staleValue.isExpired(schemaTimeout.refreshDuration)
                    )
            ) {
                iter.remove()
            }
        }
    }

    /**
     * Remove expired entries from cache.
     */
    open suspend fun pruneCache() = coroutineScope {
        launch {
            schemaCache.values.prune()
        }

        launch {
            val subjectsIter = subjectVersionCache.values.iterator()
            while (subjectsIter.hasNext()) {
                val versionMap = subjectsIter.next()
                versionMap.values.prune()
                if (versionMap.isEmpty()) {
                    subjectsIter.remove()
                }
            }
        }
    }

    /**
     * Remove all entries from cache.
     */
    open fun clearCache() {
        subjectVersionCache.clear()
        schemaCache.clear()
    }

    @RadarProducerDsl
    class Config(
        val baseUrl: String,
        var httpClient: HttpClient? = null,
    ) {
        var schemaTimeout: CacheConfig = DEFAULT_SCHEMA_TIMEOUT_CONFIG
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
        } else {
            this
        }
    }

    private fun toConfig(): Config = Config(baseUrl = baseUrl).apply {
        httpClient = this@SchemaRetriever.httpClient
        schemaTimeout = this@SchemaRetriever.schemaTimeout
        ioContext = this@SchemaRetriever.ioContext
    }

    companion object {
        private val DEFAULT_SCHEMA_TIMEOUT_CONFIG = CacheConfig(
            refreshDuration = 1.days,
            retryDuration = 1.minutes,
        )

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

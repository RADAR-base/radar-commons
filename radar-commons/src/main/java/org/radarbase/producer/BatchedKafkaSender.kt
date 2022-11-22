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
package org.radarbase.producer

import org.apache.avro.SchemaValidationException
import org.radarbase.data.AvroRecordData
import org.radarbase.data.RecordData
import org.radarbase.topic.AvroTopic
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * A Kafka REST Proxy sender that batches up records. It will send data once the batch size is
 * exceeded, or when at a send call the first record in the batch is older than given age. If send,
 * flush or close are not called within this given age, the data will also not be sent. Calling
 * [.close] will not flush or close the KafkaTopicSender that were created. That must be
 * done separately.
 *
 * @param wrappedSender kafka sender to send data with.
 * @param ageMillis threshold time after which a record should be sent.
 * @param maxBatchSize threshold batch size over which records should be sent.
*/
class BatchedKafkaSender(
    private val wrappedSender: KafkaSender,
    ageMillis: Int,
    private val maxBatchSize: Int
) : KafkaSender {
    private val ageNanos: Long

    init {
        ageNanos = TimeUnit.MILLISECONDS.toNanos(ageMillis.toLong())
    }

    @Throws(IOException::class, SchemaValidationException::class)
    override fun <K: Any, V: Any> sender(topic: AvroTopic<K, V>): KafkaTopicSender<K, V> {
        return BatchedKafkaTopicSender(topic)
    }

    @get:Throws(AuthenticationException::class)
    override val isConnected: Boolean
        get() = wrappedSender.isConnected

    @Throws(AuthenticationException::class)
    override fun resetConnection(): Boolean {
        return wrappedSender.resetConnection()
    }

    @Synchronized
    @Throws(IOException::class)
    override fun close() {
        wrappedSender.close()
    }

    /** Batched kafka topic sender. This does the actual data batching.  */
    private inner class BatchedKafkaTopicSender<K: Any, V: Any>(
        private val topic: AvroTopic<K, V>
    ) : KafkaTopicSender<K, V> {
        private var nanoAdded: Long = 0
        private var cachedKey: K? = null
        private val cache: MutableList<V> = ArrayList()
        private val topicSender: KafkaTopicSender<K, V> = wrappedSender.sender(topic)

        @Throws(IOException::class, SchemaValidationException::class)
        override fun send(key: K, value: V) {
            if (!isConnected) {
                throw IOException("Cannot send records to unconnected producer.")
            }
            trySend(key, value)
        }

        @Throws(IOException::class, SchemaValidationException::class)
        override fun send(records: RecordData<K, V>) {
            if (records.isEmpty) return
            val key = records.key
            for (value in records) {
                trySend(key, value)
            }
        }

        @Throws(IOException::class, SchemaValidationException::class)
        private fun trySend(key: K, record: V) {
            val keysMatch: Boolean
            if (cache.isEmpty()) {
                cachedKey = key
                nanoAdded = System.nanoTime()
                keysMatch = true
            } else {
                keysMatch = key == cachedKey
            }
            if (keysMatch) {
                cache.add(record)
                if (exceedsBuffer(cache)) {
                    doSend()
                }
            } else {
                doSend()
                trySend(key, record)
            }
        }

        @Throws(IOException::class, SchemaValidationException::class)
        private fun doSend() {
            val key = checkNotNull(cachedKey) { "Cached key should not be null in this function" }
            topicSender.send(AvroRecordData(topic, key, cache))
            cache.clear()
            cachedKey = null
        }

        override fun clear() {
            cache.clear()
            topicSender.clear()
        }

        @Throws(IOException::class)
        override fun flush() {
            if (cache.isNotEmpty()) {
                try {
                    doSend()
                } catch (ex: SchemaValidationException) {
                    throw IOException("Schemas do not match", ex)
                }
            }
            topicSender.flush()
        }

        @Throws(IOException::class)
        override fun close() {
            wrappedSender.use {
                flush()
            }
        }

        private fun exceedsBuffer(records: List<*>): Boolean {
            return records.size >= maxBatchSize ||
                    System.nanoTime() - nanoAdded >= ageNanos
        }
    }
}

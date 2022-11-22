package org.radarbase.producer

import org.apache.avro.SchemaValidationException
import org.radarbase.data.RecordData
import java.io.Closeable
import java.io.IOException

/**
 * Sender for a single topic. Should be created through a [KafkaSender].
 */
interface KafkaTopicSender<K: Any, V: Any> : Closeable {
    /**
     * Send a message to Kafka eventually.
     *
     * @param key key of a kafka record to send
     * @param value value of a kafka record to send
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    @Throws(IOException::class, SchemaValidationException::class)
    fun send(key: K, value: V)

    /**
     * Send a message to Kafka eventually. Contained offsets must be strictly monotonically
     * increasing for subsequent calls.
     *
     * @param records records to send.
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    @Throws(IOException::class, SchemaValidationException::class)
    fun send(records: RecordData<K, V>)

    /**
     * Clears any messages still in cache.
     */
    fun clear()

    /**
     * Flush all remaining messages.
     *
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    @Throws(IOException::class)
    fun flush()
}

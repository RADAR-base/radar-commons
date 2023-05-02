package org.radarbase.producer

import org.apache.avro.SchemaValidationException
import org.radarbase.data.AvroRecordData
import org.radarbase.data.RecordData
import org.radarbase.topic.AvroTopic
import java.io.IOException

/**
 * Sender for a single topic. Should be created through a [KafkaSender].
 */
interface KafkaTopicSender<K : Any, V : Any> {
    val topic: AvroTopic<K, V>

    /**
     * Send a message to Kafka eventually.
     *
     * @param key key of a kafka record to send
     * @param value value of a kafka record to send
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    @Throws(IOException::class, SchemaValidationException::class)
    suspend fun send(key: K, value: V) = send(key, listOf(value))

    /**
     * Send a message to Kafka eventually.
     *
     * @param key key of a kafka record to send
     * @param values values for kafka records to send
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    @Throws(IOException::class, SchemaValidationException::class)
    suspend fun send(key: K, values: List<V>) = send(AvroRecordData(topic, key, values))

    /**
     * Send a message to Kafka eventually. Contained offsets must be strictly monotonically
     * increasing for subsequent calls.
     *
     * @param records records to send.
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    @Throws(IOException::class, SchemaValidationException::class)
    suspend fun send(records: RecordData<K, V>)
}

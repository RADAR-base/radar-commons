package org.radarbase.data

import org.radarbase.topic.AvroTopic

/**
 * Avro record data belonging to a single key.
 * @param <K> key type
 * @param <V> value type
 */
/**
 * Data from a topic.
 * @param topic data topic
 * @param key data key
 * @param records non-empty data values.
 * @throws IllegalArgumentException if the values are empty.
 * @throws NullPointerException if any of the parameters are null.
 */
class AvroRecordData<K: Any, V: Any>(
    override val topic: AvroTopic<K, V>,
    override val key: K,
    private val records: List<V>,
) : RecordData<K, V> {
    init {
        require(records.isNotEmpty()) { "Records should not be empty." }
    }

    override fun iterator(): Iterator<V> = records.iterator()

    override val isEmpty: Boolean = records.isEmpty()

    override fun size(): Int = records.size
}

package org.radarbase.data

import org.radarbase.topic.AvroTopic

/**
 * Record data belonging to a single key.
 * @param <K> key type
 * @param <V> value type
</V></K> */
interface RecordData<K : Any, V : Any> : Iterable<V> {
    /**
     * Topic that the data belongs to.
     * @return Avro topic.
     */
    val topic: AvroTopic<K, V>

    /**
     * Key of each of the entries in the data set.
     * @return key
     */
    val key: K

    /** Source ID linked to record data, if any. */
    val sourceId: String?

    /**
     * Whether the list of values is empty.
     * @return true if empty, false otherwise.
     */
    val isEmpty: Boolean

    /**
     * The size of the value list.
     * @return size.
     */
    fun size(): Int
}

package org.radarbase.data;

import org.radarbase.topic.AvroTopic;

/**
 * Record data belonging to a single key.
 * @param <K> key type
 * @param <V> value type
 */
public interface RecordData<K, V> extends Iterable<V> {
    /**
     * Topic that the data belongs to.
     * @return Avro topic.
     */
    AvroTopic<K, V> getTopic();

    /**
     * Key of each of the entries in the data set.
     * @return key
     */
    K getKey();

    /**
     * Whether the list of values is empty.
     * @return true if empty, false otherwise.
     */
    boolean isEmpty();

    /**
     * The size of the value list.
     * @return size.
     */
    int size();
}

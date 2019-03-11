package org.radarbase.data;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.radarbase.topic.AvroTopic;

/**
 * Avro record data belonging to a single key.
 * @param <K> key type
 * @param <V> value type
 */
public class AvroRecordData<K, V> implements RecordData<K, V> {
    private final AvroTopic<K, V> topic;
    private final K key;
    private final List<V> records;

    /**
     * Data from a topic.
     * @param topic data topic
     * @param key data key
     * @param values non-empty data values.
     * @throws IllegalArgumentException if the values are empty.
     * @throws NullPointerException if any of the parameters are null.
     */
    public AvroRecordData(AvroTopic<K, V> topic, K key, List<V> values) {
        this.topic = Objects.requireNonNull(topic);
        this.key = Objects.requireNonNull(key);
        this.records = Objects.requireNonNull(values);
        if (this.records.isEmpty()) {
            throw new IllegalArgumentException("Records should not be empty.");
        }
    }

    @Override
    public AvroTopic<K, V> getTopic() {
        return topic;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public Iterator<V> iterator() {
        return records.iterator();
    }

    @Override
    public boolean isEmpty() {
        return records.isEmpty();
    }

    @Override
    public int size() {
        return records.size();
    }
}

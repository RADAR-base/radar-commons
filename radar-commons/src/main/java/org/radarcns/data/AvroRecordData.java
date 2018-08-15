package org.radarcns.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.topic.AvroTopic;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Avro record data belonging to a single key.
 * @param <K> key type
 * @param <V> value type
 */
public class AvroRecordData<K, V> implements RecordData<K, V> {
    private final AvroTopic<K, V> topic;
    private final K key;
    private final List<V> records;

    public AvroRecordData(AvroTopic<K, V> topic, K key, List<V> values) {
        this.topic = topic;
        this.key = key;
        this.records = values;
    }

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

    /**
     * Get an Avro encoder for given settings. This only works for
     * {@link org.apache.avro.generic.IndexedRecord} instances.
     * @param schema schema to encode with.
     * @param cls class type to encode.
     * @param binary true if the converter should yield binary data, false otherwise.
     * @param <T> type of data
     * @return new Avro writer.
     * @throws IOException if the record converter could not be created.
     * @throws IllegalArgumentException if the supplied class is not an IndexedRecord.
     */
    public static <T> AvroEncoder.AvroWriter<T> getEncoder(
            Schema schema, Class<? extends T> cls, boolean binary) throws IOException {
        AvroEncoder encoder;
        if (SpecificRecord.class.isAssignableFrom(cls)) {
            encoder = new SpecificRecordEncoder(binary);
        } else if (GenericRecord.class.isAssignableFrom(cls)) {
            encoder = new GenericRecordEncoder(binary);
        } else {
            throw new IllegalArgumentException("Cannot get encoder for non-avro records");
        }
        return encoder.writer(schema, cls);
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

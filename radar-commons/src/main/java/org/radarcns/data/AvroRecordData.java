package org.radarcns.data;

import org.radarcns.topic.AvroTopic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;

public class AvroRecordData<K, V> implements RecordData<K, V> {
    private final AvroTopic<K, V> topic;
    private final Collection<Record<K, V>> records;
    private final AvroEncoder.AvroWriter<K> keyEncoder;
    private final AvroEncoder.AvroWriter<V> valueEncoder;

    public AvroRecordData(AvroTopic<K, V> topic, Collection<Record<K, V>> records)
            throws IOException {
        this.topic = topic;
        this.records = records;
        SpecificRecordEncoder encoder = new SpecificRecordEncoder(false);
        this.keyEncoder = encoder.writer(topic.getKeySchema(), topic.getKeyClass());
        this.valueEncoder = encoder.writer(topic.getValueSchema(), topic.getValueClass());
    }

    public AvroTopic<K, V> getTopic() {
        return topic;
    }

    @Override
    public Iterator<Record<K, V>> iterator() {
        return records.iterator();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    @Override
    public Iterator<InputStream> rawIterator() {
        return new InputStreamIterator();
    }

    private class InputStreamIterator implements Iterator<InputStream> {
        private V value = null;
        private final Iterator<Record<K, V>> recordIterator;

        InputStreamIterator() {
            recordIterator = records.iterator();
        }

        @Override
        public boolean hasNext() {
            return value != null || recordIterator.hasNext();
        }

        @Override
        public InputStream next() {
            try {
                byte[] encoded;
                if (value == null) {
                    Record<K, V> record = recordIterator.next();
                    value = record.value;
                    encoded = keyEncoder.encode(record.key);
                } else {
                    encoded = valueEncoder.encode(value);
                    value = null;
                }
                return new ByteArrayInputStream(encoded);
            } catch (IOException ex) {
                throw new IllegalStateException("Cannot encode record", ex);
            }
        }

        @Override
        public void remove() {
            recordIterator.remove();
            value = null;
        }
    }
}

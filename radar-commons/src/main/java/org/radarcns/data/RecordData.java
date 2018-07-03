package org.radarcns.data;

import org.radarcns.topic.AvroTopic;

import java.io.InputStream;
import java.util.Iterator;

public interface RecordData<K, V> extends Iterable<Record<K, V>> {

    AvroTopic<K, V> getTopic();

    Iterator<InputStream> rawIterator();

    boolean isEmpty();
}

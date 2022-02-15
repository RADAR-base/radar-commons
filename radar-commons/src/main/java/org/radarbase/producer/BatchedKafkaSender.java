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

package org.radarbase.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.avro.SchemaValidationException;
import org.radarbase.data.AvroRecordData;
import org.radarbase.data.RecordData;
import org.radarbase.topic.AvroTopic;

/**
 * A Kafka REST Proxy sender that batches up records. It will send data once the batch size is
 * exceeded, or when at a send call the first record in the batch is older than given age. If send,
 * flush or close are not called within this given age, the data will also not be sent. Calling
 * {@link #close()} will not flush or close the KafkaTopicSender that were created. That must be
 * done separately.
 */
public class BatchedKafkaSender implements KafkaSender {
    private final KafkaSender wrappedSender;
    private final long ageNanos;
    private final int maxBatchSize;

    /**
     * Kafka sender that sends data along.
     * @param sender kafka sender to send data with.
     * @param ageMillis threshold time after which a record should be sent.
     * @param maxBatchSize threshold batch size over which records should be sent.
     */
    public BatchedKafkaSender(KafkaSender sender, int ageMillis, int maxBatchSize) {
        this.wrappedSender = sender;
        this.ageNanos = TimeUnit.MILLISECONDS.toNanos(ageMillis);
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public <K, V> KafkaTopicSender<K, V> sender(final AvroTopic<K, V> topic)
            throws IOException, SchemaValidationException {
        return new BatchedKafkaTopicSender<>(topic);
    }

    @Override
    public boolean isConnected() throws AuthenticationException {
        return wrappedSender.isConnected();
    }

    @Override
    public boolean resetConnection() throws AuthenticationException {
        return wrappedSender.resetConnection();
    }

    @Override
    public synchronized void close() throws IOException {
        wrappedSender.close();
    }

    /** Batched kafka topic sender. This does the actual data batching. */
    private class BatchedKafkaTopicSender<K, V> implements KafkaTopicSender<K, V> {
        private long nanoAdded;
        private K cachedKey;
        private final List<V> cache;
        private final KafkaTopicSender<K, V> topicSender;
        private final AvroTopic<K, V> topic;

        private BatchedKafkaTopicSender(AvroTopic<K, V> topic)
                throws IOException, SchemaValidationException {
            cache = new ArrayList<>();
            this.topic = topic;
            topicSender = wrappedSender.sender(topic);
        }

        @Override
        public void send(K key, V value) throws IOException, SchemaValidationException {
            if (!isConnected()) {
                throw new IOException("Cannot send records to unconnected producer.");
            }
            trySend(key, value);
        }

        @Override
        public void send(RecordData<K, V> records) throws IOException, SchemaValidationException {
            if (records.isEmpty()) {
                return;
            }
            K key = records.getKey();
            for (V value : records) {
                trySend(key, value);
            }
        }

        private void trySend(K key, V record) throws IOException, SchemaValidationException {
            boolean keysMatch;

            if (cache.isEmpty()) {
                cachedKey = key;
                nanoAdded = System.nanoTime();
                keysMatch = true;
            } else {
                keysMatch = Objects.equals(key, cachedKey);
            }

            if (keysMatch) {
                cache.add(record);
                if (exceedsBuffer(cache)) {
                    doSend();
                }
            } else {
                doSend();
                trySend(key, record);
            }
        }

        private void doSend() throws IOException, SchemaValidationException {
            topicSender.send(new AvroRecordData<>(topic, cachedKey, cache));
            cache.clear();
            cachedKey = null;
        }

        @Override
        public void clear() {
            cache.clear();
            topicSender.clear();
        }

        @Override
        public void flush() throws IOException {
            if (!cache.isEmpty()) {
                try {
                    doSend();
                } catch (SchemaValidationException ex) {
                    throw new IOException("Schemas do not match", ex);
                }
            }
            topicSender.flush();
        }

        @Override
        @SuppressWarnings("PMD.UseTryWithResources")
        public void close() throws IOException {
            try {
                flush();
            } finally {
                wrappedSender.close();
            }
        }

        private boolean exceedsBuffer(List<?> records) {
            return records.size() >= maxBatchSize
                    || System.nanoTime() - nanoAdded >= ageNanos;
        }
    }
}

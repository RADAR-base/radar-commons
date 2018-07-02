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

package org.radarcns.producer;

import org.radarcns.data.AvroRecordData;
import org.radarcns.data.Record;
import org.radarcns.data.RecordData;
import org.radarcns.topic.AvroTopic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Kafka REST Proxy sender that batches up records. It will send data once the batch size is
 * exceeded, or when at a send call the first record in the batch is older than given age. If send,
 * flush or close are not called within this given age, the data will also not be sent. Calling
 * {@link #close()} will not flush or close the KafkaTopicSender that were created. That must be
 * done separately.
 */
public class BatchedKafkaSender implements KafkaSender {
    private final KafkaSender wrappedSender;
    private final int ageMillis;
    private final int maxBatchSize;

    public BatchedKafkaSender(KafkaSender sender, int ageMillis, int maxBatchSize) {
        this.wrappedSender = sender;
        this.ageMillis = ageMillis;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public <K, V> KafkaTopicSender<K, V> sender(final AvroTopic<K, V> topic)
            throws IOException {
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

    private class BatchedKafkaTopicSender<K, V> implements
            KafkaTopicSender<K, V> {
        private final List<Record<K, V>> cache;
        private final KafkaTopicSender<K, V> topicSender;
        private final AvroTopic<K, V> topic;

        private BatchedKafkaTopicSender(AvroTopic<K, V> topic) throws IOException {
            cache = new ArrayList<>();
            this.topic = topic;
            topicSender = wrappedSender.sender(topic);
        }

        @Override
        public void send(K key, V value) throws IOException {
            if (!isConnected()) {
                throw new IOException("Cannot send records to unconnected producer.");
            }
            trySend(new Record<>(key, value));
        }

        @Override
        public void send(RecordData<K, V> records) throws IOException {
            if (records.isEmpty()) {
                return;
            }
            for (Record<K, V> record : records) {
                trySend(record);
            }
        }

        private void trySend(Record<K, V> record) throws IOException {
            boolean doSend;
            if (record == null) {
                doSend = !cache.isEmpty();
            } else {
                cache.add(record);
                doSend = exceedsBuffer(cache);
            }

            if (doSend) {
                topicSender.send(new AvroRecordData<>(topic, cache));
                cache.clear();
            }
        }

        @Override
        public void clear() {
            cache.clear();
            topicSender.clear();
        }

        @Override
        public void flush() throws IOException {
            trySend(null);
            topicSender.flush();
        }

        @Override
        public void close() throws IOException {
            try {
                flush();
            } finally {
                wrappedSender.close();
            }
        }

        private boolean exceedsBuffer(List<Record<K, V>> records) {
            return records.size() >= maxBatchSize
                    || System.currentTimeMillis() - records.get(0).milliTimeAdded >= ageMillis;
        }
    }
}

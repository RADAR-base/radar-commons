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

import org.radarcns.data.Record;
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
 *
 * @param <K> base key class
 * @param <V> base value class
 */
public class BatchedKafkaSender<K, V> implements KafkaSender<K, V> {
    private final KafkaSender<K, V> wrappedSender;
    private final int ageMillis;
    private final int maxBatchSize;

    public BatchedKafkaSender(KafkaSender<K, V> sender, int ageMillis, int maxBatchSize) {
        this.wrappedSender = sender;
        this.ageMillis = ageMillis;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public <L extends K, W extends V> KafkaTopicSender<L, W> sender(final AvroTopic<L, W> topic)
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

    private class BatchedKafkaTopicSender<L extends K, W extends V> implements
            KafkaTopicSender<L, W> {
        private final List<Record<L, W>> cache;
        private final KafkaTopicSender<L, W> topicSender;

        private BatchedKafkaTopicSender(AvroTopic<L, W> topic) throws IOException {
            cache = new ArrayList<>();
            topicSender = wrappedSender.sender(topic);
        }

        @Override
        public void send(long offset, L key, W value) throws IOException {
            if (!isConnected()) {
                throw new IOException("Cannot send records to unconnected producer.");
            }
            cache.add(new Record<>(offset, key, value));

            if (exceedsBuffer(cache)) {
                topicSender.send(cache);
                cache.clear();
            }
        }

        @Override
        public void send(List<Record<L, W>> records) throws IOException {
            if (records.isEmpty()) {
                return;
            }
            if (cache.isEmpty()) {
                if (exceedsBuffer(records)) {
                    topicSender.send(records);
                } else {
                    cache.addAll(records);
                }
            } else {
                cache.addAll(records);

                if (exceedsBuffer(cache)) {
                    topicSender.send(cache);
                    cache.clear();
                }
            }
        }

        @Override
        public long getLastSentOffset() {
            return topicSender.getLastSentOffset();
        }

        @Override
        public void clear() {
            cache.clear();
            topicSender.clear();
        }

        @Override
        public void flush() throws IOException {
            if (!cache.isEmpty()) {
                topicSender.send(cache);
                cache.clear();
            }
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

        private boolean exceedsBuffer(List<Record<L, W>> records) {
            return records.size() >= maxBatchSize
                    || System.currentTimeMillis() - records.get(0).milliTimeAdded >= ageMillis;
        }
    }
}

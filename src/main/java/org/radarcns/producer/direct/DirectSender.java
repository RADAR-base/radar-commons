/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.producer.direct;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.radarcns.data.Record;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.topic.AvroTopic;

/**
 * Directly sends a message to Kafka using a KafkaProducer
 */
public class DirectSender<K, V> implements KafkaSender<K, V> {
    private final KafkaProducer<K, V> producer;

    public DirectSender(Properties properties) {

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public <L extends K, W extends V> KafkaTopicSender<L, W> sender(final AvroTopic<L, W> topic)
            throws IOException {
        return new DirectTopicSender<>(topic);
    }

    @Override
    public boolean resetConnection() {
        return true;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private class DirectTopicSender<L extends K, W extends V> implements KafkaTopicSender<L, W> {
        private long lastOffset = -1L;
        private final AvroTopic<L, W> topic;

        private DirectTopicSender(AvroTopic<L, W> topic) {
            this.topic = topic;
        }

        @Override
        public void send(long offset, L key, W value) throws IOException {
            producer.send(new ProducerRecord<>(topic.getName(), (K)key, (V)value));

            lastOffset = offset;
        }

        @Override
        public void send(List<Record<L, W>> records) throws IOException {
            for (Record<L, W> record : records) {
                producer.send(new ProducerRecord<K, V>(topic.getName(), record.key, record.value));
            }
            lastOffset = records.get(records.size() - 1).offset;
        }

        @Override
        public long getLastSentOffset() {
            return lastOffset;
        }

        @Override
        public void clear() {
            // noop
        }

        @Override
        public void flush() throws IOException {
            producer.flush();
        }

        @Override
        public void close() throws IOException {
            producer.flush();
        }
    }
}

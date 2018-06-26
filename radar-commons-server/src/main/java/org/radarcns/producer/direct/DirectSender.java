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

package org.radarcns.producer.direct;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.radarcns.data.Record;
import org.radarcns.data.RecordData;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.topic.AvroTopic;

import java.util.Properties;

/**
 * Directly sends a message to Kafka using a KafkaProducer
 */
public class DirectSender implements KafkaSender {
    private final KafkaProducer producer;

    public DirectSender(Properties properties) {
        producer = new KafkaProducer(properties);
    }

    @Override
    public <K, V> KafkaTopicSender<K, V> sender(final AvroTopic<K, V> topic) {
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

    @SuppressWarnings("unchecked")
    private class DirectTopicSender<K, V> implements KafkaTopicSender<K, V> {
        private final String name;

        private DirectTopicSender(AvroTopic<K, V> topic) {
            name = topic.getName();
        }

        @Override
        public void send(K key, V value) {
            producer.send(new ProducerRecord<>(name, key, value));
            producer.flush();
        }

        @Override
        public void send(RecordData<K, V> records) {
            for (Record<K, V> record : records) {
                producer.send(new ProducerRecord<>(name, record.key, record.value));
            }
            producer.flush();
        }

        @Override
        public void clear() {
            // noop
        }

        @Override
        public void flush() {
            // noop
        }

        @Override
        public void close() {
            // noop
        }
    }
}

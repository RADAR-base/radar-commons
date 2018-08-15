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

import org.apache.avro.SchemaValidationException;
import org.radarcns.topic.AvroTopic;

import java.io.Closeable;
import java.io.IOException;

/**
 * Thread-safe sender. Calling {@link #close()} must be done after all {@link KafkaTopicSender}
 * senders created with {@link #sender(AvroTopic)} have been called.
 */
public interface KafkaSender extends Closeable {
    /** Get a non thread-safe sender instance. */
    <K, V> KafkaTopicSender<K, V> sender(AvroTopic<K, V> topic)
            throws IOException, SchemaValidationException;

    /**
     * If the sender is no longer connected, try to reconnect.
     * @return whether the connection has been restored.
     */
    boolean resetConnection() throws AuthenticationException;

    /**
     * Whether the sender is connected to the Kafka system.
     */
    boolean isConnected() throws AuthenticationException;
}

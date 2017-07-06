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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface KafkaTopicSender<K, V> extends Closeable {
    /**
     * Send a message to Kafka eventually. Given offset must be strictly monotonically increasing
     * for subsequent calls.
     *
     * @param offset local offset, monotonically increasing
     * @param key key of a kafka record to send
     * @param value value of a kafka record to send
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    void send(long offset, K key, V value) throws IOException;

    /**
     * Send a message to Kafka eventually.
     *
     * Contained offsets must be strictly monotonically increasing
     * for subsequent calls.
     *
     * @param records records to send.
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    void send(List<Record<K, V>> records) throws IOException;

    /**
     * Get the latest offsets actually sent for a given topic. Returns -1L for unknown offsets.
     */
    long getLastSentOffset();

    /**
     * Clears any messages still in cache.
     */
    void clear();

    /**
     * Flush all remaining messages.
     *
     * @throws AuthenticationException if the client failed to authenticate itself
     * @throws IOException if the client could not send a message
     */
    void flush() throws IOException;
}

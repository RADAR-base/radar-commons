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
package org.radarbase.producer

import org.apache.avro.SchemaValidationException
import org.radarbase.topic.AvroTopic
import java.io.Closeable
import java.io.IOException

/**
 * Thread-safe sender. Calling [.close] must be done after all [KafkaTopicSender]
 * senders created with [.sender] have been called.
 */
interface KafkaSender : Closeable {
    /** Get a non thread-safe sender instance.  */
    @Throws(IOException::class, SchemaValidationException::class)
    fun <K: Any, V: Any> sender(topic: AvroTopic<K, V>): KafkaTopicSender<K, V>

    /**
     * If the sender is no longer connected, try to reconnect.
     * @return whether the connection has been restored.
     * @throws AuthenticationException if the headers caused an authentication error
     * in the current request or in a previous one.
     */
    @Throws(AuthenticationException::class)
    fun resetConnection(): Boolean

    /**
     * Get the current connection state to Kafka. If the connection state is unknown, this will
     * trigger a connection check.
     * @return true if connected, false if not connected.
     * @throws AuthenticationException if the headers caused an authentication error
     * in a previous request or during an additional connection
     * check.
     */
    @get:Throws(AuthenticationException::class)
    val isConnected: Boolean
}

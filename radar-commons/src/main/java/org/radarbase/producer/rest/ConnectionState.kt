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
package org.radarbase.producer.rest

import java.util.concurrent.TimeUnit

/**
 * Current connection status of a KafkaSender. After a timeout occurs this will turn to
 * disconnected. When the connection is dropped, the associated KafkaSender should set this to
 * disconnected, when it successfully connects, it should set it to connected. This class is
 * thread-safe. The state transition diagram is CONNECTED to and from DISCONNECTED with
 * [.didConnect] and [.didDisconnect]; CONNECTED to and from UNKNOWN with
 * [.getState] after a timeout occurs and [.didConnect]; and UNKNOWN to DISCONNECTED
 * with [.didDisconnect].
 *
 *
 * A connection state could be shared with multiple HTTP clients if they are talking to the same
 * server.
 *
 * @param timeout timeout
 * @param unit unit of the timeout
 * @throws IllegalArgumentException if the timeout is not strictly positive.
 */
class ConnectionState(
    timeout: Long,
    unit: TimeUnit,
) {
    /** State symbols of the connection.  */
    enum class State {
        CONNECTED, DISCONNECTED, UNKNOWN, UNAUTHORIZED
    }

    private var timeout: Long = TimeUnit.MILLISECONDS.convert(timeout, unit)
    private var lastConnection: Long = -1L

    /** Current state of the connection.  */
    @get:Synchronized
    var state: State = State.UNKNOWN
        get() {
            if (field == State.CONNECTED && System.currentTimeMillis() - lastConnection >= timeout) {
                field = State.UNKNOWN
            }
            return field
        }
        private set

    /** For a sender to indicate that a connection attempt succeeded.  */
    @Synchronized
    fun didConnect() {
        state = State.CONNECTED
        lastConnection = System.currentTimeMillis()
    }

    /** For a sender to indicate that a connection attempt failed.  */
    @Synchronized
    fun didDisconnect() {
        state = State.DISCONNECTED
    }

    @Synchronized
    fun wasUnauthorized() {
        state = State.UNAUTHORIZED
    }

    @Synchronized
    fun reset() {
        state = State.UNKNOWN
    }

    /**
     * Set the timeout after which the state will go from CONNECTED to UNKNOWN.
     * @param timeout timeout
     * @param unit unit of the timeout
     * @throws IllegalArgumentException if the timeout is not strictly positive
     */
    @Synchronized
    fun setTimeout(timeout: Long, unit: TimeUnit) {
        require(timeout > 0) { "Timeout must be strictly positive" }
        this.timeout = TimeUnit.MILLISECONDS.convert(timeout, unit)
    }
}

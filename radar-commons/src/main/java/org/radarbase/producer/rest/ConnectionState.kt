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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.map
import org.radarbase.util.TimedValue
import org.radarbase.util.TimeoutConfig

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
 * @param timeoutConfig timeout config
 * @throws IllegalArgumentException if the timeout is not strictly positive.
 */
class ConnectionState(
    private val timeoutConfig: TimeoutConfig,
) {
    /** State symbols of the connection.  */
    enum class State {
        CONNECTED, DISCONNECTED, UNKNOWN, UNAUTHORIZED
    }

    val state: Flow<State>
        get() = mutableState.map {
            if (it.value === State.CONNECTED && it.isExpired)
                State.UNKNOWN
            else
                it.value
        }

    private val mutableState = MutableStateFlow(TimedValue(State.UNKNOWN, timeoutConfig))

    init {
        mutableState.tryEmit(TimedValue(State.UNKNOWN, timeoutConfig))
    }

    /** For a sender to indicate that a connection attempt succeeded.  */
    suspend fun didConnect() {
        mutableState.emit(TimedValue(State.CONNECTED, timeoutConfig))
    }

    /** For a sender to indicate that a connection attempt failed.  */
    suspend fun didDisconnect() {
        mutableState.emit(TimedValue(State.DISCONNECTED, timeoutConfig))
    }

    suspend fun wasUnauthorized() {
        mutableState.emit(TimedValue(State.UNAUTHORIZED, timeoutConfig))
    }

    suspend fun reset() {
        mutableState.emit(TimedValue(State.UNKNOWN, timeoutConfig))
    }
}

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

import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds

class ConnectionStateTest {
    @Test
    @Timeout(1, unit = TimeUnit.SECONDS)
    fun testState() = runBlocking {
        var state = ConnectionState(10.milliseconds)
        state.assertEqualTo(ConnectionState.State.UNKNOWN)
        state.didConnect()
        state.assertEqualTo(ConnectionState.State.CONNECTED)
        state.didDisconnect()
        state.assertEqualTo(ConnectionState.State.DISCONNECTED)
        delay(15.milliseconds)
        state.assertEqualTo(ConnectionState.State.DISCONNECTED)
        state.didConnect()
        delay(15.milliseconds)
        state.assertEqualTo(ConnectionState.State.UNKNOWN)
        state.scope.cancel()
        state = ConnectionState(25.milliseconds)
        state.didConnect()
        state.assertEqualTo(ConnectionState.State.CONNECTED)
        delay(10.milliseconds)
        state.assertEqualTo(ConnectionState.State.CONNECTED)
        delay(20.milliseconds)
        state.assertEqualTo(ConnectionState.State.UNKNOWN)
        state.scope.cancel()
    }

    private suspend inline fun ConnectionState.assertEqualTo(expected: ConnectionState.State) {
        assertEquals(expected, state.first())
    }
}

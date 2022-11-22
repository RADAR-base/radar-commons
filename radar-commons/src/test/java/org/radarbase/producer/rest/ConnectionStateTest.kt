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

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.last
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.radarbase.util.TimeoutConfig
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit

class ConnectionStateTest {
    @Test
    @Timeout(1, unit = TimeUnit.SECONDS)
    fun testState() = runBlocking {
        var state = ConnectionState(TimeoutConfig(Duration.ofMillis(10)))
        logger.info("initial state set")
        state.assertEqualTo(ConnectionState.State.UNKNOWN)
        logger.info("setting to didConnect")
        state.didConnect()
        state.assertEqualTo(ConnectionState.State.CONNECTED)
        state.didDisconnect()
        state.assertEqualTo(ConnectionState.State.DISCONNECTED)
        delay(15)
        state.assertEqualTo(ConnectionState.State.DISCONNECTED)
        state.didConnect()
        delay(15)
        state.assertEqualTo(ConnectionState.State.UNKNOWN)
        state = ConnectionState(TimeoutConfig(Duration.ofMillis(25)))
        state.didConnect()
        state.assertEqualTo(ConnectionState.State.CONNECTED)
        delay(10)
        state.assertEqualTo(ConnectionState.State.CONNECTED)
        delay(15)
        state.assertEqualTo(ConnectionState.State.UNKNOWN)
    }

    private suspend fun ConnectionState.assertEqualTo(expected: ConnectionState.State) {
        assertEquals(expected, state.first())
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ConnectionStateTest::class.java)
    }
}

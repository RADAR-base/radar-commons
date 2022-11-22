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

import org.junit.Assert
import org.junit.Test
import java.util.concurrent.TimeUnit

class ConnectionStateTest {
    @Test
    fun testState() {
        val state = ConnectionState(10, TimeUnit.MILLISECONDS)
        Assert.assertEquals(ConnectionState.State.UNKNOWN, state.state)
        state.didConnect()
        Assert.assertEquals(ConnectionState.State.CONNECTED, state.state)
        state.didDisconnect()
        Assert.assertEquals(ConnectionState.State.DISCONNECTED, state.state)
        Thread.sleep(15)
        Assert.assertEquals(ConnectionState.State.DISCONNECTED, state.state)
        state.didConnect()
        Assert.assertEquals(ConnectionState.State.CONNECTED, state.state)
        Thread.sleep(10)
        Assert.assertEquals(ConnectionState.State.UNKNOWN, state.state)
        state.setTimeout(25, TimeUnit.MILLISECONDS)
        state.didConnect()
        Assert.assertEquals(ConnectionState.State.CONNECTED, state.state)
        Thread.sleep(10)
        Assert.assertEquals(ConnectionState.State.CONNECTED, state.state)
        Thread.sleep(15)
        Assert.assertEquals(ConnectionState.State.UNKNOWN, state.state)
    }
}

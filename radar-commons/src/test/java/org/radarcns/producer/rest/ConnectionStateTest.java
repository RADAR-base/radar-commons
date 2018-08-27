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

package org.radarcns.producer.rest;

import static org.junit.Assert.assertEquals;
import static org.radarcns.producer.rest.ConnectionState.State.CONNECTED;
import static org.radarcns.producer.rest.ConnectionState.State.DISCONNECTED;
import static org.radarcns.producer.rest.ConnectionState.State.UNKNOWN;

import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ConnectionStateTest {
    @Test
    public void getState() throws Exception {
        ConnectionState state = new ConnectionState(10, TimeUnit.MILLISECONDS);
        assertEquals(UNKNOWN, state.getState());
        state.didConnect();
        assertEquals(CONNECTED, state.getState());
        state.didDisconnect();
        assertEquals(DISCONNECTED, state.getState());
        Thread.sleep(15);
        assertEquals(DISCONNECTED, state.getState());
        state.didConnect();
        assertEquals(CONNECTED, state.getState());
        Thread.sleep(10);
        assertEquals(UNKNOWN, state.getState());
        state.setTimeout(25, TimeUnit.MILLISECONDS);
        state.didConnect();
        assertEquals(CONNECTED, state.getState());
        Thread.sleep(10);
        assertEquals(CONNECTED, state.getState());
        Thread.sleep(15);
        assertEquals(UNKNOWN, state.getState());
    }
}

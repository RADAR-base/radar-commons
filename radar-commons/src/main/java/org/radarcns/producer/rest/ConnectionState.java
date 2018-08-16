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

import java.util.concurrent.TimeUnit;

/**
 * Current connection status of a KafkaSender. After a timeout occurs this will turn to
 * disconnected. When the connection is dropped, the associated KafkaSender should set this to
 * disconnected, when it successfully connects, it should set it to connected. This class is
 * thread-safe. The state transition diagram is CONNECTED to and from DISCONNECTED with
 * {@link #didConnect()} and {@link #didDisconnect()}; CONNECTED to and from UNKNOWN with
 * {@link #getState()} after a timeout occurs and {@link #didConnect()}; and UNKNOWN to DISCONNECTED
 * with {@link #didDisconnect()}.
 *
 * <p>A connection state could be shared with multiple HTTP clients if they are talking to the same
 * server.
 */
public final class ConnectionState {

    /** State symbols of the connection. */
    public enum State {
        CONNECTED, DISCONNECTED, UNKNOWN, UNAUTHORIZED
    }

    private long timeout;
    private long lastConnection;
    private State state;

    /**
     * Connection state with given timeout. The state will start as connected.
     * @param timeout timeout
     * @param unit unit of the timeout
     * @throws IllegalArgumentException if the timeout is not strictly positive.
     */
    public ConnectionState(long timeout, TimeUnit unit) {
        lastConnection = -1L;
        state = State.UNKNOWN;
        setTimeout(timeout, unit);
    }

    /** Current state of the connection. */
    public synchronized State getState() {
        if (state == State.CONNECTED && System.currentTimeMillis() - lastConnection >= timeout) {
            state = State.UNKNOWN;
        }
        return state;
    }

    /** For a sender to indicate that a connection attempt succeeded. */
    public synchronized void didConnect() {
        state = State.CONNECTED;
        lastConnection = System.currentTimeMillis();
    }

    /** For a sender to indicate that a connection attempt failed. */
    public synchronized void didDisconnect() {
        state = State.DISCONNECTED;
    }

    public synchronized void wasUnauthorized() {
        state = State.UNAUTHORIZED;
    }

    public synchronized void reset() {
        state = State.UNKNOWN;
    }

    /**
     * Set the timeout after which the state will go from CONNECTED to UNKNOWN.
     * @param timeout timeout
     * @param unit unit of the timeout
     * @throws IllegalArgumentException if the timeout is not strictly positive
     */
    public synchronized void setTimeout(long timeout, TimeUnit unit) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be strictly positive");
        }
        this.timeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
    }
}

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

import okhttp3.ConnectionPool;

/**
 * Manages a connection pool. Using this class properly ensures that all resources are released
 * after a global ConnectionPool is no longer used. Each call to {@link #acquire()} must be matched
 * with exactly one call to {@link #release()} once the acquired ConnectionPool is no longer used.
 * This class is thread-safe.
 */
public class ManagedConnectionPool {
    public static final ManagedConnectionPool GLOBAL_POOL = new ManagedConnectionPool();
    private ConnectionPool connectionPool;
    private int references;

    public ManagedConnectionPool() {
        references = 0;
    }

    /**
     * Acquire access to a connection pool. A call to this must be matched with exactly one call
     * to {@link #release()}.
     * @return the single connection pool managed by this object
     */
    public synchronized ConnectionPool acquire() {
        if (references == 0) {
            connectionPool = new ConnectionPool();
        }
        references++;
        return connectionPool;
    }

    /**
     * Release access to a connection pool once it is no longer used.
     * @throws IllegalStateException if release is called more often than acquire.
     */
    public synchronized void release() {
        if (references == 0) {
            throw new IllegalStateException(
                    "Cannot release a connection pool that was not acquired.");
        }
        references--;
        if (references == 0) {
            connectionPool = null;
        }
    }
}

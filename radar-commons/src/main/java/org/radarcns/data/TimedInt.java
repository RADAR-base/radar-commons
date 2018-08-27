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

package org.radarcns.data;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single int, with modification times timed with system milliseconds time.
 * This class can be used from multiple threads.
 */
public class TimedInt {
    private final AtomicInteger value = new AtomicInteger();
    private final AtomicLong time = new AtomicLong(-1L);

    /**
     * Value of the int.
     */
    public int getValue() {
        return value.get();
    }

    /**
     * Time that the int got modified.
     */
    public long getTime() {
        return time.get();
    }

    /**
     * Add value to the int. This updates the time variable to now.
     * @param delta value to add.
     */
    public void add(int delta) {
        value.addAndGet(delta);
        time.set(System.currentTimeMillis());
    }

    /**
     * Set value to the int. This updates the time variable to now.
     * @param value new value
     */
    public void set(int value) {
        this.value.set(value);
        time.set(System.currentTimeMillis());
    }

    @Override
    public synchronized boolean equals(Object other) {
        if (other == null || !getClass().equals(other.getClass())) {
            return false;
        }
        TimedInt timedOther = (TimedInt)other;
        return value.equals(timedOther.value) && time.equals(timedOther.time);
    }

    @Override
    public int hashCode() {
        return 31 * value.hashCode() + time.hashCode();
    }
}

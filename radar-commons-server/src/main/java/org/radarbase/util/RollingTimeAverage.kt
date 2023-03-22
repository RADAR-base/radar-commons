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
package org.radarbase.util

import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.math.roundToInt

/**
 * Get the average of a set of values collected in a sliding time window of fixed duration. At least
 * one value is needed to get an average.
 * @param window duration of the time window.
 */
class RollingTimeAverage(
    private val window: Duration,
) {
    private var firstTime: TimeCount? = null
    private val deque: Deque<TimeCount> = LinkedList()

    /** Whether values have already been added.  */
    val hasAverage: Boolean
        get() = firstTime != null

    /** Add a new value.  */
    fun add(x: Double) {
        if (firstTime == null) {
            firstTime = TimeCount(x)
        } else {
            deque.addLast(TimeCount(x))
        }
    }

    /** Add a value of one.  */
    fun increment() {
        add(1.0)
    }

    /**
     * Get the average value per second over a sliding time window of fixed size.
     *
     * It takes one value before the window started as a baseline, and adds all values in the
     * window. It then divides by the total time window from the first value (outside/before the
     * window) to the last value (at the end of the window).
     * @return average value per second
     */
    val average: Double
        get() {
            var localFirstTime = checkNotNull(firstTime) { "Cannot get average without values" }
            val now = Instant.now()
            val windowStart = now - window
            while (!deque.isEmpty() && deque.first.time < windowStart) {
                localFirstTime = deque.removeFirst()
            }
            val total = localFirstTime.value + deque.sumOf { it.value }
            firstTime = localFirstTime
            return if (deque.isEmpty() || localFirstTime.time >= windowStart) {
                1000.0 * total / Duration.between(localFirstTime.time, now).toMillis()
            } else {
                val time = Duration.between(windowStart, deque.last.time)
                val removedRate = Duration.between(localFirstTime.time, windowStart).toMillis() /
                    Duration.between(localFirstTime.time, deque.first.time).toMillis().toDouble()
                val removedValue = localFirstTime.value + deque.first.value * removedRate
                1000.0 * (total - removedValue) / time.toMillis()
            }
        }

    /**
     * Rounded [.getAverage].
     */
    val count: Int
        get() = average.roundToInt()

    private class TimeCount(
        val value: Double,
    ) {
        val time: Instant = Instant.now()
    }
}

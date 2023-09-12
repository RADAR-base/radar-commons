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

import kotlin.math.roundToLong
import kotlin.time.ComparableTimeMark
import kotlin.time.Duration
import kotlin.time.TimeSource

/**
 * Get the average of a set of values collected in a sliding time window of fixed duration. At least
 * one value is needed to get an average. This class is not thread-safe and must be synchronized
 * externally if accessed from multiple threads.
 * @param window duration of the time window.
 */
class RollingTimeAverage(
    private val window: Duration,
) {
    private var firstTimeCount: TimeCount? = null
    private val deque = ArrayDeque<TimeCount>()

    /** Whether values have already been added.  */
    val hasAverage: Boolean
        get() = firstTimeCount != null

    /** Add a new value.  */
    fun add(x: Double) {
        if (firstTimeCount == null) {
            firstTimeCount = TimeCount(x)
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
     * @return average value per second or 0 if no value is present
     */
    val average: Double
        get() {
            var firstTimeCount = firstTimeCount ?: return 0.0
            val now = TimeSource.Monotonic.markNow()
            val windowStart = now - window
            var firstInDeque = deque.firstOrNull()
            while (firstInDeque != null && firstInDeque.time < windowStart) {
                firstTimeCount = deque.removeFirst()
                firstInDeque = deque.firstOrNull()
            }
            if (firstTimeCount !== this.firstTimeCount) {
                this.firstTimeCount = firstTimeCount
            }
            val total = firstTimeCount.value + deque.sumOf { it.value }
            val firstTime = firstTimeCount.time
            return if (firstInDeque == null || firstTime >= windowStart) {
                1000.0 * total / (now - firstTime).inWholeMilliseconds
            } else {
                val duration = deque.last().time - windowStart
                val removedRate = (windowStart - firstTime).inWholeMilliseconds /
                    (firstInDeque.time - firstTime).inWholeMilliseconds.toDouble()
                val removedValue = firstTimeCount.value + firstInDeque.value * removedRate
                1000.0 * (total - removedValue) / duration.inWholeMilliseconds
            }
        }

    /**
     * Rounded [RollingTimeAverage.average].
     */
    val count: Long
        get() = average.roundToLong()

    private class TimeCount(
        val value: Double,
    ) {
        val time: ComparableTimeMark = TimeSource.Monotonic.markNow()
    }
}

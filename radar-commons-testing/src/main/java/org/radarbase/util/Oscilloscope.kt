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

import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeMark
import kotlin.time.TimeSource

/**
 * Oscilloscope gives out a regular beat, at a given frequency per second. The intended way to use
 * this is with a do-while loop, with the [.beat] retrieved at the start of the loop, and
 * [.willRestart] in the condition of the loop.
 */
class Oscilloscope(
    private val frequency: Int,
) {
    private val baseTime: TimeMark = TimeSource.Monotonic.markNow()
    private var iteration: AtomicInteger = AtomicInteger(0)

    /** Whether the next beat will restart at one.  */
    fun willRestart(): Boolean = iteration.get() % frequency == 0

    /**
     * One oscilloscope beat, sleeping if necessary to not exceed the frequency per second. The beat
     * number starts at one, increases to the frequency, and then goes to one again.
     * @return one up to the given frequency
     * @throws InterruptedException when the sleep was interrupted.
     */
    @Throws(InterruptedException::class)
    suspend fun beat(): Int {
        val currentIteration = iteration.getAndIncrement()
        val timeToSleep = currentIteration.seconds / frequency - baseTime.elapsedNow()
        if (timeToSleep > Duration.ZERO) {
            logger.info("delaying {} millis", timeToSleep.inWholeMilliseconds)
            delay(timeToSleep)
        }
        return currentIteration % frequency + 1
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Oscilloscope::class.java)
    }
}

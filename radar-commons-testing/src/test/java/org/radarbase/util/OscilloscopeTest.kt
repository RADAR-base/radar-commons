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

import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.Test

class OscilloscopeTest {
    @Test
    @Throws(Exception::class)
    fun beat() = runBlocking {
        val oscilloscope = Oscilloscope(128)
        val time = System.currentTimeMillis()
        var iteration = 1
        do {
            val beat = oscilloscope.beat()
            assertThat(beat, `is`(iteration++))
            if (beat == 2) {
                // time of one beat is about 1/128 seconds = 7.8125 milliseconds
                val beatDiff = System.currentTimeMillis() - time
                assertThat(beatDiff, greaterThanOrEqualTo(5L))
                assertThat(beatDiff, lessThanOrEqualTo(14L))
            }
        } while (!oscilloscope.willRestart())

        // frequency must match
        assertThat(iteration, `is`(129))
        // restarts every frequency, the willRestart function does not reset
        assertThat(oscilloscope.willRestart(), `is`(true))
        // beat starts at 1 again
        assertThat(oscilloscope.beat(), `is`(1))
        // total time, from one cycle to the next, is about 1 second
        val cycleDiff = System.currentTimeMillis() - time
        assertThat(cycleDiff, greaterThanOrEqualTo(996L))
        assertThat(cycleDiff, lessThan(1020L))
    }
}

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

/**
 * Dynamically computes timestamps for the given amount of samples with the given frequency.
 * Timestamps start back in time, and the last timestamp will be equal to the time that this
 * function was called. If samples is set to 0, this function will start at the current time
 * and move forward, generating an infinite number of samples.
 *
 * @param samples number of timestamp that has to be generated, 0 to generate infinite samples
 * @param frequency number of samples that the sensor generates in 1 second, strictly positive
 */
class Metronome(
    private val samples: Long,
    private val frequency: Int,
) {
    private val baseTime: Long
    private var iteration: Long = 0

    init {
        require(samples >= 0) { "The amount of samples must be positive" }
        require(frequency > 0) { "Frequency must be larger than zero" }
        baseTime = if (samples == 0L) {
            System.currentTimeMillis()
        } else {
            System.currentTimeMillis() - (samples - 1).toTimeOffset()
        }
    }

    /** Whether the metronome will generate another sample.  */
    operator fun hasNext(): Boolean = samples == 0L || iteration < samples

    /** Generate the next sample.  */
    operator fun next(): Long {
        check(hasNext()) { "Iterator finished" }

        return baseTime + (iteration++).toTimeOffset()
    }

    private fun Long.toTimeOffset(): Long = this * 1000 / frequency
}

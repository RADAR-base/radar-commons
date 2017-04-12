/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.util;

import java.util.concurrent.TimeUnit;

/**
 * Dynamically computes create for the given amount of samples with the given frequency.
 * Timestamps start back in time, and the last timestamp will be equal to the time that this
 * function was called. If samples is set to 0, this function will start at the current time
 * and move forward, generating an infinite number of samples.
 */
public final class Metronome {
    private final long timeStep;
    private final long samples;
    private final long baseTime;
    private long iteration;

    /**
     * Construct a Metronome.
     *
     * @param samples number of timestamp that has to be generated, 0 to generate infinite samples
     * @param frequency number of samples that the sensor generates in 1 second, strictly positive
     */
    public Metronome(long samples, int frequency) {
        if (samples < 0) {
            throw new IllegalArgumentException("The amount of samples must be positve");
        }
        if (frequency <= 0) {
            throw new IllegalArgumentException("Frequency must be larger than zero");
        }

        long shift = samples / frequency;

        this.samples = samples;
        this.baseTime = TimeUnit.MILLISECONDS.toNanos(
                System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(shift));
        this.timeStep = 1_000_000_000L / frequency;
        this.iteration = 0;
    }

    /** Whether the metronome will generate another sample. */
    public boolean hasNext() {
        return iteration < samples || samples == 0;
    }

    /** Generate the next sample. */
    public long next() {
        if (!hasNext()) {
            throw new IllegalStateException("Iterator finished");
        }
        return TimeUnit.NANOSECONDS.toMillis(baseTime + iteration++ * timeStep);
    }
}

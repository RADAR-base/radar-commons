package org.radarcns.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/*
 *  Copyright 2016 Kings College London and The Hyve
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

/**
 * Tool to simulate time.
 */
public class Metronome {

    private Metronome() {}

    //private static final Logger LOGGER = LoggerFactory.getLogger(Metronome.class);

    /**
     * Computes timestamps for the given amount of samples with the given frequency.
     * @param samples number of timestamp that has to be generated
     * @param frequency number of samples that the sensor generates in 1 second
     * @param baseFrequency used in case the sensor is on board a device with other sensors having
     *      different frequencies. This must be the maximum between all supported frequencies.
     * @return a list of timestamps
     */
    public static List<Long> timestamps(int samples, int frequency, int baseFrequency) {
        checkInput(samples, frequency, baseFrequency);

        List<Long> timestamps = new ArrayList<>();

        long shift = samples / frequency;
        final long baseTime = TimeUnit.MILLISECONDS.toNanos(
                System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(shift));
        final long timeStep = 1_000_000_000L / baseFrequency;

        int iteration = 0;

        while (timestamps.size() < samples) {
            if ((iteration % baseFrequency + 1) % (baseFrequency / frequency) == 0) {
                timestamps.add(timestamps.size(), TimeUnit.NANOSECONDS.toMillis(
                        baseTime + iteration * timeStep));
            }

            iteration++;
        }

        return timestamps;
    }

    /**
     * Checks whether the input parameters are valid input or not.
     */
    private static void checkInput(int samples, int frequency, int baseFrequency) {
        if (samples < 0) {
            throw new IllegalArgumentException("The amount of samples must be positve");
        }

        if (frequency < 0) {
            throw new IllegalArgumentException("Frequency must be bigger than zero");
        }

        if (baseFrequency < frequency) {
            throw new IllegalArgumentException("BaseFrequency cannot be smaller than frequency");
        }
    }
}

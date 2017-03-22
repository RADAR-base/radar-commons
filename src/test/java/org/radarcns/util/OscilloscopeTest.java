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

import static org.junit.Assert.*;

import org.hamcrest.Matchers;
import org.junit.Test;

public class OscilloscopeTest {
    @Test
    public void beat() throws Exception {
        Oscilloscope oscilloscope = new Oscilloscope(128);

        long time = System.currentTimeMillis();
        int iteration = 1;
        do {
            int beat = oscilloscope.beat();
            assertEquals(iteration++, beat);
            if (beat == 2) {
                // time of one beat is about 1/128 seconds = 7.8125 milliseconds
                long beatDiff = System.currentTimeMillis() - time;
                assertThat(beatDiff, Matchers.greaterThanOrEqualTo(7L));
                assertThat(beatDiff, Matchers.lessThan(12L));
            }
        } while (!oscilloscope.willRestart());

        // frequency must match
        assertEquals(iteration, 129);
        // restarts every frequency, the willRestart function does not reset
        assertTrue(oscilloscope.willRestart());
        // beat starts at 1 again
        assertEquals(1, oscilloscope.beat());
        // total time, from one cycle to the next, is about 1 second
        long cycleDiff = System.currentTimeMillis() - time;
        assertThat(cycleDiff, Matchers.greaterThanOrEqualTo(998L));
        assertThat(cycleDiff, Matchers.lessThan(1004L));
    }
}
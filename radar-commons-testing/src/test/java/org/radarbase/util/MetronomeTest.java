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

package org.radarbase.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class MetronomeTest {
    private void check(Metronome it, long expectedMin) {
        assertThat(it.hasNext(), is(true));
        long t = it.next();
        assertThat(t, greaterThanOrEqualTo(expectedMin));
        assertThat(t, lessThan(expectedMin + 50L));
    }

    @Test
    public void timestamps() {
        long base = System.currentTimeMillis();
        Metronome it = new Metronome(5, 2);
        check(it, base - 2000L);
        check(it, base - 1500L);
        check(it, base - 1000L);
        check(it, base - 500L);
        check(it, base);
        assertThat(it.hasNext(), is(false));
        assertThrows(IllegalStateException.class, it::next);
    }

    @Test
    public void errFreqTimestamps() {
        assertThrows(IllegalArgumentException.class, () -> new Metronome(5, -1));
    }

    @Test
    public void errFreq0Timestamps() {
        assertThrows(IllegalArgumentException.class, () -> new Metronome(5, 0));
    }

    @Test
    public void errSampleTimestamps() {
        assertThrows(IllegalArgumentException.class, () -> new Metronome(-1, 10));
    }

    @Test
    public void sample0Timestamps() {
        long base = System.currentTimeMillis();
        Metronome it = new Metronome(0, 10);
        check(it, base);
        check(it, base + 100L);
        check(it, base + 200L);
        check(it, base + 300L);
    }
}

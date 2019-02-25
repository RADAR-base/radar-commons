/*
 * Copyright 2017 King's College London and The Hyve
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

package org.radarcns.stream.collector;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.radarcns.passive.empatica.EmpaticaE4Acceleration;

/**
 * Created by nivethika on 20-12-16.
 */
public class AggregateListCollectorTest {
    @Test
    public void add() {
        AggregateListCollector arrayCollector = new AggregateListCollector(
                new String[]{"a", "b", "c", "d"}, false);
        double[] arrayvalues = {0.15d, 1.0d, 2.0d, 3.0d};
        arrayCollector.add(arrayvalues);
        assertEquals(4, arrayCollector.getCollectors().size());
        assertEquals(0.15, arrayCollector.getCollectors().get(0).getMin(), 0.0d);
        assertEquals(1.0, arrayCollector.getCollectors().get(1).getMin(), 0.0d);
        assertEquals(2.0, arrayCollector.getCollectors().get(2).getMin(), 0.0d);
        assertEquals(3.0, arrayCollector.getCollectors().get(3).getMin(), 0.0d);
    }

    @Test
    public void addRecord() {
        AggregateListCollector arrayCollector = new AggregateListCollector(new String[] {"x", "y", "z"},
                EmpaticaE4Acceleration.getClassSchema(), false);
        arrayCollector.add(new EmpaticaE4Acceleration(0d, 0d, 0.15f, 1.0f, 2.0f));

        assertEquals(3, arrayCollector.getCollectors().size());
        assertEquals(0.15, arrayCollector.getCollectors().get(0).getMin(), 0.0d);
        assertEquals(1.0, arrayCollector.getCollectors().get(1).getMin(), 0.0d);
        assertEquals(2.0, arrayCollector.getCollectors().get(2).getMin(), 0.0d);
    }
}

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
                new String[]{"a", "b", "c", "d"});
        double[] arrayvalues = {0.15d, 1.0d, 2.0d, 3.0d};
        arrayCollector.add(arrayvalues);
        assertEquals("[DoubleValueCollector{name=a, min=0.15, max=0.15, sum=0.15, count=1, mean=0.15, quartile=[0.15, 0.15, 0.15], history=[0.15]}, DoubleValueCollector{name=b, min=1.0, max=1.0, sum=1.0, count=1, mean=1.0, quartile=[1.0, 1.0, 1.0], history=[1.0]}, DoubleValueCollector{name=c, min=2.0, max=2.0, sum=2.0, count=1, mean=2.0, quartile=[2.0, 2.0, 2.0], history=[2.0]}, DoubleValueCollector{name=d, min=3.0, max=3.0, sum=3.0, count=1, mean=3.0, quartile=[3.0, 3.0, 3.0], history=[3.0]}]" , arrayCollector.toString());
    }

    @Test
    public void addRecord() {
        AggregateListCollector arrayCollector = new AggregateListCollector(new String[] {"x", "y", "z"},
                EmpaticaE4Acceleration.getClassSchema());
        arrayCollector.add(new EmpaticaE4Acceleration(0d, 0d, 0.15f, 1.0f, 2.0f));
        assertEquals("[DoubleValueCollector{name=x, min=0.15, max=0.15, sum=0.15, count=1, mean=0.15, quartile=[0.15, 0.15, 0.15], history=[0.15]}, DoubleValueCollector{name=y, min=1.0, max=1.0, sum=1.0, count=1, mean=1.0, quartile=[1.0, 1.0, 1.0], history=[1.0]}, DoubleValueCollector{name=z, min=2.0, max=2.0, sum=2.0, count=1, mean=2.0, quartile=[2.0, 2.0, 2.0], history=[2.0]}]" , arrayCollector.toString());
    }
}

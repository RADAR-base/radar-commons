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

package org.radarcns.mock;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Test;
import org.radarcns.data.Record;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.mock.data.RecordGenerator;

/**
 * Created by joris on 17/05/2017.
 */
public class RecordGeneratorTest {

    @Test
    public void generate() throws Exception {
        MockDataConfig config = new MockDataConfig();
        config.setTopic("test");
        config.setFrequency(10);
        config.setMinimum(0.1);
        config.setMaximum(9.9);
        config.setValueFields(Arrays.asList("x", "y", "z"));
        config.setValueSchema(EmpaticaE4Acceleration.class.getName());

        RecordGenerator<MeasurementKey> generator = new RecordGenerator<>(config,
                MeasurementKey.class);
        Iterator<Record<MeasurementKey, SpecificRecord>> iter = generator
                .iterateValues(new MeasurementKey("a", "b"), 0);
        Record<MeasurementKey, SpecificRecord> record = iter.next();
        assertEquals(0, record.offset);
        assertEquals(new MeasurementKey("a", "b"), record.key);
        float x = ((EmpaticaE4Acceleration)record.value).getX();
        assertTrue(x >= 0.1f && x < 9.9f);
        float y = ((EmpaticaE4Acceleration)record.value).getX();
        assertTrue(y >= 0.1f && y < 9.9f);
        float z = ((EmpaticaE4Acceleration)record.value).getX();
        assertTrue(z >= 0.1f && z < 9.9f);
        double time = ((EmpaticaE4Acceleration)record.value).getTime();
        assertTrue(time > System.currentTimeMillis() / 1000d - 1d
                && time <= System.currentTimeMillis() / 1000d);

        Record<MeasurementKey, SpecificRecord> nextRecord = iter.next();
        assertEquals(1, nextRecord.offset);
        assertEquals(time + 0.1d, (Double)nextRecord.value.get(0), 1e-6);
    }

    @Test
    public void getHeaders() throws Exception {
        MockDataConfig config = new MockDataConfig();
        config.setTopic("test");
        config.setValueSchema(EmpaticaE4Acceleration.class.getName());

        RecordGenerator<MeasurementKey> generator = new RecordGenerator<>(config,
                MeasurementKey.class);
        assertEquals(
                Arrays.asList("userId", "sourceId", "time", "timeReceived", "x", "y", "z"),
                generator.getHeader());
    }
}
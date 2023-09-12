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

package org.radarbase.mock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;
import org.radarbase.data.Record;
import org.radarbase.mock.config.MockDataConfig;
import org.radarbase.mock.data.RecordGenerator;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.empatica.EmpaticaE4Acceleration;

/**
 * Created by joris on 17/05/2017.
 */
public class RecordGeneratorTest {

    @Test
    public void generate() {
        MockDataConfig config = new MockDataConfig();
        config.topic = "test";
        config.frequency = 10;
        config.minimum = 0.1;
        config.maximum = 9.9;
        config.valueFields = Arrays.asList("x", "y", "z");
        config.valueSchema = EmpaticaE4Acceleration.class.getName();

        RecordGenerator<ObservationKey> generator = new RecordGenerator<>(config,
                ObservationKey.class);
        Iterator<Record<ObservationKey, SpecificRecord>> iter = generator
                .iterateValues(new ObservationKey("test", "a", "b"), 0);
        Record<ObservationKey, SpecificRecord> record = iter.next();
        assertEquals(new ObservationKey("test", "a", "b"), record.getKey());
        float x = ((EmpaticaE4Acceleration)record.getValue()).getX();
        assertTrue(x >= 0.1f && x < 9.9f);
        float y = ((EmpaticaE4Acceleration)record.getValue()).getX();
        assertTrue(y >= 0.1f && y < 9.9f);
        float z = ((EmpaticaE4Acceleration)record.getValue()).getX();
        assertTrue(z >= 0.1f && z < 9.9f);
        double time = ((EmpaticaE4Acceleration)record.getValue()).getTime();
        long now = System.currentTimeMillis();
        assertThat(time, greaterThan(now / 1000d - 1d));
        assertThat(time, lessThanOrEqualTo(now / 1000d));

        Record<ObservationKey, SpecificRecord> nextRecord = iter.next();
        assertEquals(time + 0.1d, (Double)nextRecord.getValue().get(0), 1e-6);
    }

    @Test
    public void getHeaders() {
        MockDataConfig config = new MockDataConfig();
        config.topic = "test";
        config.valueSchema = EmpaticaE4Acceleration.class.getName();

        RecordGenerator<ObservationKey> generator = new RecordGenerator<>(config,
                ObservationKey.class);
        assertEquals(
                Arrays.asList("key.projectId", "key.userId", "key.sourceId",
                        "value.time", "value.timeReceived", "value.x", "value.y", "value.z"),
                generator.getHeader());
    }
}

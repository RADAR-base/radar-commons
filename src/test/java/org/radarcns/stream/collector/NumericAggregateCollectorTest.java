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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.monitor.application.ApplicationRecordCounts;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.passive.phone.PhoneBatteryLevel;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Created by nivethika on 20-12-16.
 */
public class NumericAggregateCollectorTest {

    private NumericAggregateCollector valueCollector;

    @Before
    public void setUp() {
        this.valueCollector = new NumericAggregateCollector.Builder("test")
                .build();
    }

    @Test
    public void add() {
        valueCollector.add(10.0d);
        assertEquals(10.0d, valueCollector.getMin(), 0.0d);
        assertEquals(10.0d, valueCollector.getMax(), 0.0d);
        assertEquals(10.0d, valueCollector.getSum(), 0.0d);
        assertEquals(10.0d, valueCollector.getMean(), 0.0d);
        assertEquals(0.0d, valueCollector.getInterQuartileRange(), 0.0d);
        assertEquals(1, valueCollector.getCount(),0);

        valueCollector.add(15.100d);
        assertEquals(10.0d, valueCollector.getMin(), 0.0d);
        assertEquals(15.100d, valueCollector.getMax(), 0.0d);
        assertEquals(25.100d, valueCollector.getSum(), 0.0d);
        assertEquals(12.550d, valueCollector.getMean(), 0.0d);
        assertEquals(5.1, valueCollector.getInterQuartileRange(), 0.0d);
        assertEquals(2, valueCollector.getCount(),0);

        valueCollector.add(28.100d);
        assertEquals(18.1d, valueCollector.getInterQuartileRange(), 0.0d);

    }

    @Test
    public void addFloat() {
        valueCollector.add(10.0234f);
        assertEquals(10.0234d, valueCollector.getMin(), 0.0d);
        assertEquals(10.0234d, valueCollector.getMax(), 0.0d);
        assertEquals(10.0234d, valueCollector.getSum(), 0.0d);
        assertEquals(10.0234d, valueCollector.getMean(), 0.0d);
        assertEquals(0.0d, valueCollector.getInterQuartileRange(), 0.0d);
        assertEquals(1, valueCollector.getCount(),0);

        valueCollector.add(15.0d);
        assertEquals(10.0234d, valueCollector.getMin(), 0.0d);
        assertEquals(15.0d, valueCollector.getMax(), 0.0d);
        assertEquals(25.0234d, valueCollector.getSum(), 0.0d);
        assertEquals(12.5117d, valueCollector.getMean(), 0.0d);
        assertEquals(4.9766d, valueCollector.getInterQuartileRange(), 0.0d);
        assertEquals(2, valueCollector.getCount(),0);

        valueCollector.add(28.100d);
        assertEquals(18.0766d, valueCollector.getInterQuartileRange(), 0.0d);

    }

    @Test
    public void testAverage() {
        double[] input = {36.793899922141186, 36.878288191353626, 36.965575690177715, 36.988087035729855, 36.628622572158214};
        for (double d : input) {
            valueCollector.add(d);
        }
        assertEquals(36.850894682312116, valueCollector.getMean(), 0);
    }

    @Test
    public void testAverageFloat() {
        double[] input = {36.793899922141186, 36.878288191353626, 36.965575690177715, 36.988087035729855, 36.628622572158214};
        for (double d : input) {
            valueCollector.add((float)d);
        }
        // converting to float will give a lower number of decimals on the double result
        assertEquals(36.8508954, valueCollector.getMean(), 0);
    }

    @Test(expected = IllegalStateException.class)
    public void testAddRecordWithoutSchema() {
        valueCollector.add(new EmpaticaE4BloodVolumePulse(0d, 0d, 0f));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongRecordType() {
        this.valueCollector = new NumericAggregateCollector("isPlugged",
                PhoneBatteryLevel.getClassSchema());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongFieldName() {
        this.valueCollector = new NumericAggregateCollector("doesNotExist",
                PhoneBatteryLevel.getClassSchema());
    }

    @Test
    public void testRecordType() {
        this.valueCollector = new NumericAggregateCollector("batteryLevel",
                PhoneBatteryLevel.getClassSchema());
        this.valueCollector = new NumericAggregateCollector("time",
                PhoneBatteryLevel.getClassSchema());
        this.valueCollector = new NumericAggregateCollector("recordsSent",
                ApplicationRecordCounts.getClassSchema());
        this.valueCollector = new NumericAggregateCollector("recordsCached",
                ApplicationRecordCounts.getClassSchema());
        this.valueCollector = new NumericAggregateCollector("start",
                AggregateKey.getClassSchema());
    }

    @Test
    public void testAddRecord() {
        this.valueCollector = new NumericAggregateCollector("bloodVolumePulse", EmpaticaE4BloodVolumePulse.getClassSchema());
        valueCollector.add(new EmpaticaE4BloodVolumePulse(0d, 0d, 0f));
        assertEquals(1, valueCollector.getCount());
        assertEquals(0d, valueCollector.getMean(), 0d);
    }

    @Test
    public void testAddRecordWithNull() {
        this.valueCollector = new NumericAggregateCollector("recordsCached", ApplicationRecordCounts.getClassSchema());
        valueCollector.add(new ApplicationRecordCounts(0d, 1, 0, 1));
        assertEquals(1, valueCollector.getCount());
        assertEquals(1d, valueCollector.getMean(), 1e-5d);
        valueCollector.add(new ApplicationRecordCounts(0d, null, 0, 1));
        assertEquals(1, valueCollector.getCount());
        assertEquals(1d, valueCollector.getMean(), 1d);
        valueCollector.add(new ApplicationRecordCounts(0d, 2, 0, 1));
        assertEquals(2, valueCollector.getCount());
        assertEquals(1.5d, valueCollector.getMean(), 1e-5d);
    }

    @Test
    public void testSerialization() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        valueCollector = new NumericAggregateCollector.Builder("test")
                .history(Arrays.asList(-1d, 15d))
                .sum(BigDecimal.valueOf(14))
                .build();

        String valueString = mapper.writeValueAsString(valueCollector);
        System.out.println(valueString);
        assertEquals(valueCollector, mapper.readValue(valueString, NumericAggregateCollector.class));
    }

    @Test
    public void testReservoirBuilder() {
        valueCollector = new NumericAggregateCollector.Builder("test")
                .reservoir(new UniformSamplingReservoir(Arrays.asList(-1d, 15d), 2, 1))
                .build();

        assertEquals(2, valueCollector.getCount());
        assertEquals(1, valueCollector.getReservoir().getSamples().size());
    }

    @Test
    public void testReservoirBuilderUnlimited() {
        valueCollector = new NumericAggregateCollector.Builder("name")
                .reservoir(new UniformSamplingReservoir(Arrays.asList(-1d, 15d), 2, 1000))
                .build();

        assertEquals(2, valueCollector.getCount());
        assertEquals(2, valueCollector.getReservoir().getSamples().size());
    }
}

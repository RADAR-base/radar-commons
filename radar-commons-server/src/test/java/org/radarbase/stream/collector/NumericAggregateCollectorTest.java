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

package org.radarbase.stream.collector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.monitor.application.ApplicationRecordCounts;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.passive.phone.PhoneBatteryLevel;

/**
 * Created by nivethika on 20-12-16.
 */
public class NumericAggregateCollectorTest {

    private NumericAggregateCollector valueCollector;

    @BeforeEach
    public void setUp() {
        this.valueCollector = new NumericAggregateCollector("test", true);
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

    @Test
    public void testAddRecordWithoutSchema() {
        assertThrows(IllegalStateException.class, () ->
            valueCollector.add(new EmpaticaE4BloodVolumePulse(0d, 0d, 0f))
        );
    }

    @Test
    public void testWrongRecordType() {
        assertThrows(IllegalArgumentException.class, () ->
                this.valueCollector = new NumericAggregateCollector("isPlugged",
                        PhoneBatteryLevel.getClassSchema(), false)
        );
    }

    @Test
    public void testWrongFieldName() {
        assertThrows(IllegalArgumentException.class, () ->
                this.valueCollector = new NumericAggregateCollector("doesNotExist",
                        PhoneBatteryLevel.getClassSchema(), false)
        );
    }

    @Test
    public void testRecordType() {
        this.valueCollector = new NumericAggregateCollector("batteryLevel",
                PhoneBatteryLevel.getClassSchema(), false);
        this.valueCollector = new NumericAggregateCollector("time",
                PhoneBatteryLevel.getClassSchema(), false);
        this.valueCollector = new NumericAggregateCollector("recordsSent",
                ApplicationRecordCounts.getClassSchema(), false);
        this.valueCollector = new NumericAggregateCollector("recordsCached",
                ApplicationRecordCounts.getClassSchema(), false);
        this.valueCollector = new NumericAggregateCollector("timeStart",
                AggregateKey.getClassSchema(), false);
    }

    @Test
    public void testAddRecord() {
        this.valueCollector = new NumericAggregateCollector("bloodVolumePulse", EmpaticaE4BloodVolumePulse.getClassSchema(), false);
        valueCollector.add(new EmpaticaE4BloodVolumePulse(0d, 0d, 0f));
        assertEquals(1, valueCollector.getCount());
        assertEquals(0d, valueCollector.getMean(), 0d);
    }

    @Test
    public void testAddRecordWithNull() {
        this.valueCollector = new NumericAggregateCollector("recordsCached", ApplicationRecordCounts.getClassSchema(), false);
        valueCollector.add(new ApplicationRecordCounts(0d, 1L, 0L, 1));
        assertEquals(1, valueCollector.getCount());
        assertEquals(1d, valueCollector.getMean(), 1e-5d);
        valueCollector.add(new ApplicationRecordCounts(0d, null, 0L, 1));
        assertEquals(1, valueCollector.getCount());
        assertEquals(1d, valueCollector.getMean(), 1d);
        valueCollector.add(new ApplicationRecordCounts(0d, 2L, 0L, 1));
        assertEquals(2, valueCollector.getCount());
        assertEquals(1.5d, valueCollector.getMean(), 1e-5d);
    }

    @Test
    public void testSerialization() {
        valueCollector = new NumericAggregateCollector();
        NumericAggregateState state = new NumericAggregateState();
        state.setName("test");
        state.setCount(2L);
        state.setMin(-1d);
        state.setMax(15d);
        state.setSum(new BigDecimalState(ByteBuffer.wrap(BigInteger.valueOf(14).toByteArray()), 0));
        state.setReservoir(new SamplingReservoirState(Arrays.asList(-1d, 15d), 2L, 999));
        valueCollector.fromAvro(state);
        assertEquals(state, valueCollector.toAvro());
    }

    @Test
    public void testReservoirBuilder() {
        valueCollector = new NumericAggregateCollector();
        NumericAggregateState state = new NumericAggregateState();
        state.setName("test");
        state.setCount(2L);
        state.setMin(-1d);
        state.setMax(15d);
        state.setSum(new BigDecimalState(ByteBuffer.wrap(BigInteger.valueOf(14).toByteArray()), 0));
        state.setReservoir(new SamplingReservoirState(Arrays.asList(-1d, 15d), 2L, 1));
        valueCollector.fromAvro(state);

        assertEquals(2, valueCollector.getCount());
        assertEquals(1, valueCollector.getReservoir().getSamples().size());
        assertEquals(2, valueCollector.getReservoir().getCount());
        assertEquals(1, valueCollector.getReservoir().getMaxSize());
    }

    @Test
    public void testReservoirBuilderUnlimited() {
        valueCollector = new NumericAggregateCollector();
        NumericAggregateState state = new NumericAggregateState();
        state.setName("test");
        state.setCount(2L);
        state.setMin(-1d);
        state.setMax(15d);
        state.setSum(new BigDecimalState(ByteBuffer.wrap(BigInteger.valueOf(14).toByteArray()), 0));
        state.setReservoir(new SamplingReservoirState(Arrays.asList(-1d, 15d), 2L, 1000));
        valueCollector.fromAvro(state);

        assertEquals(2, valueCollector.getCount());
        assertEquals(2, valueCollector.getReservoir().getSamples().size());
    }
}

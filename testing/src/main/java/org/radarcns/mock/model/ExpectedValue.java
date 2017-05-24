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

package org.radarcns.mock.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.key.MeasurementKey;

/**
 * It computes the expected value for a test case.
 */
public abstract class ExpectedValue<V> {
    // Timewindow length in milliseconds
    public static final long DURATION = TimeUnit.SECONDS.toMillis(10);
    private final int timeReceivedPos;
    private final int[] valuePos;

    private Long lastTimestamp;
    private MeasurementKey lastKey;
    private V lastValue;
    private final Map<Long, V> series;

    /**
     * Constructor.
     **/
    public ExpectedValue(Schema valueSchema, List<String> valueFields) {
        timeReceivedPos = valueSchema.getField("timeReceived").pos();

        valuePos = new int[valueFields.size()];
        for (int i = 0; i < valuePos.length; i++) {
            valuePos[i] = valueSchema.getField(valueFields.get(i)).pos();
        }

        series = new HashMap<>();
        lastTimestamp = 0L;
        lastValue = null;
    }

    /**
     * Constructor.
     **/
    public ExpectedValue() {
        timeReceivedPos = -1;
        valuePos = null;

        series = new HashMap<>();
        lastTimestamp = 0L;
        lastValue = null;
    }

    /**
     * Create a new value for the series. This is called when the time window of a record does not
     * match a previous value in the time series.
     */
    protected abstract V createValue();

    /**
     * Add record to a single time series value.
     * @param value value to add record to.
     * @param values values of the record to add
     */
    protected abstract void addToValue(V value, Object[] values);

    /** Number of value fields of the records in this expected value. */
    public int getValueFieldLength() {
        return valuePos.length;
    }

    protected Object getValue(SpecificRecord record, int i) {
        return record.get(valuePos[i]);
    }

    public Map<Long, V> getSeries() {
        return series;
    }

    /**
     * Add a new record to the series of expected values.
     * @param record record to add
     */
    public void add(Record<MeasurementKey, SpecificRecord> record) {
        if (timeReceivedPos == -1) {
            throw new IllegalStateException("Cannot parse record without a schema.");
        }
        long timeMillis = (long) ((Double) record.value.get(timeReceivedPos) / 1000d);
        Object[] obj = new Object[valuePos.length];
        for (int i = 0; i < valuePos.length; i++) {
            obj[i] = record.value.get(valuePos[i]);
        }

        add(record.key, timeMillis, obj);
    }

    /**
     * Add a new record to the series of expected values.
     * @param key measurement key
     * @param timeMillis time the record is received
     * @param values values to add
     */
    public void add(MeasurementKey key, long timeMillis, Object... values) {
        this.lastKey = key;
        if (timeMillis >= lastTimestamp + DURATION || lastValue == null) {
            lastTimestamp = timeMillis - (timeMillis % DURATION);
            lastValue = createValue();
            getSeries().put(lastTimestamp, lastValue);
        }
        addToValue(lastValue, values);
    }

    public MeasurementKey getLastKey() {
        return lastKey;
    }
}

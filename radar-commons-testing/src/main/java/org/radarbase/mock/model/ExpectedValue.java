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

package org.radarbase.mock.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.radarbase.data.Record;
import org.radarbase.stream.collector.RecordCollector;

/**
 * It computes the expected value for a test case.
 */
public abstract class ExpectedValue<V extends RecordCollector> {
    // Timewindow length in milliseconds
    public static final long DURATION = TimeUnit.SECONDS.toMillis(10);
    private final int timeReceivedPos;
    protected final Schema schema;
    protected final String[] fieldNames;

    private Long lastTimestamp;
    private V lastCollector;
    private final Map<Long, V> series;

    /**
     * Constructor.
     **/
    public ExpectedValue(Schema valueSchema, List<String> fieldNames) {
        this.schema = valueSchema;
        this.fieldNames = fieldNames.toArray(new String[0]);
        timeReceivedPos = valueSchema.getField("timeReceived").pos();

        series = new HashMap<>();
        lastTimestamp = 0L;
        lastCollector = null;
    }

    /**
     * Create a new value for the series. This is called when the time window of a record does not
     * match a previous value in the time series.
     */
    protected abstract V createCollector();

    public Map<Long, V> getSeries() {
        return series;
    }

    /**
     * Add a new record to the series of expected values.
     * @param record record to add
     */
    public void add(Record<GenericRecord, GenericRecord> record) {
        if (timeReceivedPos == -1) {
            throw new IllegalStateException("Cannot parse record without a schema.");
        }
        long timeMillis = (long) ((Double) record.getValue().get(timeReceivedPos) * 1000d);
        if (timeMillis >= lastTimestamp + DURATION || lastCollector == null) {
            lastTimestamp = timeMillis - (timeMillis % DURATION);
            lastCollector = createCollector();
            getSeries().put(lastTimestamp, lastCollector);
        }
        lastCollector.add(record.getValue());
    }
}

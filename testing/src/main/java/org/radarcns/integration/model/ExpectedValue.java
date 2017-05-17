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

package org.radarcns.integration.model;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.radarcns.key.MeasurementKey;

/**
 * It computes the expected value for a test case.
 */
public class ExpectedValue<V> {

    public enum StatType {
        MINIMUM,
        MAXIMUM,
        SUM,
        QUARTILES,
        MEDIAN,
        INTERQUARTILE_RANGE,
        COUNT,
        AVERAGE
    }

    //Timewindow length in milliseconds
    @SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:MemberName"})
    public static final long DURATION = TimeUnit.SECONDS.toMillis(10);

    protected Long lastTimestamp;
    protected V lastValue;
    private final Map<Long, V> series;
    private final MeasurementKey key;

    /**
     * Constructor.
     **/
    public ExpectedValue(MeasurementKey key, Class<V> valueClass)
            throws IllegalAccessException, InstantiationException {
        series = new HashMap<>();

        this.key = key;
        lastTimestamp = 0L;

        lastValue = valueClass.newInstance();
    }

    public Map<Long, V> getSeries() {
        return series;
    }

    public MeasurementKey getKey() {
        return key;
    }
}

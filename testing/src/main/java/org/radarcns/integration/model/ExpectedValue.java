/*
 * Copyright 2017 Kings College London and The Hyve
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


import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.radarcns.stream.aggregator.DoubleArrayCollector;
import org.radarcns.stream.aggregator.DoubleValueCollector;


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

    /**
     * Enumerator containing all possible collector implementations. Useful to understand if
     * the current isntance is managing single doubles or arrays of doubles.
     *
     * {@link org.radarcns.stream.aggregator.DoubleArrayCollector}
     * {@link DoubleValueCollector}
     **/
    public enum ExpectedType {
        ARRAY(DoubleArrayCollector.class.getCanonicalName()),
        DOUBLE(DoubleValueCollector.class.getCanonicalName());

        private String value;

        ExpectedType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return this.getValue();
        }

        /**
         * Return the {@code ExpectedType} associated to the input String.
         *
         * @param value representing an {@code ExpectedType} item
         * @return the {@code ExpectedType} that matches the input
         **/
        public static ExpectedType getEnum(String value) {
            for (ExpectedType v : values()) {
                if (v.getValue().equalsIgnoreCase(value)) {
                    return v;
                }
            }
            throw new IllegalArgumentException();
        }
    }

    //Timewindow length in milliseconds
    @SuppressWarnings({"checkstyle:AbbreviationAsWordInName", "checkstyle:MemberName"})
    public static final long DURATION = TimeUnit.SECONDS.toMillis(10);

    private String user;
    private String source;

    protected Long lastTimestamp;
    protected V lastValue;
    private HashMap<Long, V> series;

    /**
     * Constructor.
     **/
    public ExpectedValue(String user, String source)
            throws IllegalAccessException, InstantiationException {
        series = new HashMap<>();

        this.user = user;
        this.source = source;
        lastTimestamp = 0L;

        Class<V> valueClass = (Class<V>) ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments()[0];

        lastValue = valueClass.newInstance();
    }

    public ExpectedType getExpectedType() {
        for (ExpectedType expectedType : ExpectedType.values()) {
            if (expectedType.getValue().equals(lastValue.getClass().getCanonicalName())) {
                return expectedType;
            }
        }

        return null;
    }

    public String getUser() {
        return user;
    }

    public String getSource() {
        return source;
    }

    public HashMap<Long, V> getSeries() {
        return series;
    }
}

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

import org.radarcns.integration.aggregator.DoubleValueCollector;

/**
 * {@code ExpectedValue} represented as {@code Double}.
 *
 * {@link ExpectedValue}
 */
public class ExpectedDoubleValue extends ExpectedValue<DoubleValueCollector> {

    //private static final Logger logger = LoggerFactory.getLogger(ExpectedDoubleValue.class);

    /**
     * Constructor.
     **/
    public ExpectedDoubleValue(String user, String source)
            throws InstantiationException, IllegalAccessException {
        super(user, source);
    }

    /**
     * It adds a new value the simulation taking into account if it belongs to an existing time
     * window or not.
     *
     * @param startTimeWindow timeZero for a time window that has this sample as initil value
     * @param timestamp time associated with the value
     * @param value sample value
     **/
    public void add(Long startTimeWindow, Long timestamp, double value) {
        if (timestamp < lastTimestamp + DURATION) {
            lastValue.add(value);
        } else {
            lastTimestamp = startTimeWindow;
            lastValue = new DoubleValueCollector();
            lastValue.add(value);
            getSeries().put(startTimeWindow, lastValue);
        }
    }
}

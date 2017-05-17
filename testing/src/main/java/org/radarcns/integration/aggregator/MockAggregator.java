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

package org.radarcns.integration.aggregator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.radarcns.integration.model.ExpectedArrayValue;
import org.radarcns.integration.model.ExpectedDoubleValue;
import org.radarcns.integration.model.ExpectedValue;
import org.radarcns.integration.model.MockConfigToCsvParser;
import org.radarcns.integration.model.MockRecord;
import org.radarcns.mock.MockDataConfig;

/**
 * The MockAggregator simulates the behaviour of a Kafka Streams application based on time window.
 * It supported accumulators are <ul>
 * <li>array of {@code Double}
 * <li>singleton {@code Double}
 * </ul>
 */
public final class MockAggregator {
    /**
     * Default constructor.
     */
    private MockAggregator() {}

    /**
     * @param parser class that reads a CVS file line by line returning an {@code Map} .
     * @return {@code ExpectedArrayValue} the simulated results computed using the input parser.
     * {@link org.radarcns.integration.model.ExpectedArrayValue}
     **/
    public static ExpectedArrayValue simulateArrayCollector(MockConfigToCsvParser parser)
            throws IOException, IllegalAccessException, InstantiationException {

        MockRecord.DoubleArrayType record = parser.nextDoubleArrayRecord();
        ExpectedArrayValue eav = new ExpectedArrayValue(record.getKey());

        while (record != null) {
            eav.add(record.getTimeWindow(10_000),
                    record.getTimeMillis(),
                    record.getValues());

            record = parser.nextDoubleArrayRecord();
        }

        return eav;
    }

    /**
     * @param parser class that reads a CSV file line by line returning an {@code HashMap}.
     * @return {@code ExpectedDoubleValue} the simulated results computed using the input parser.
     * {@link ExpectedDoubleValue}
     **/
    public static ExpectedDoubleValue simulateSingletonCollector(MockConfigToCsvParser parser)
            throws IOException, IllegalAccessException, InstantiationException {

        MockRecord.DoubleType record = parser.nextDoubleRecord();
        ExpectedDoubleValue edv = new ExpectedDoubleValue(record.getKey());

        while (record != null) {
            edv.add(record.getTimeWindow(10_000),
                    record.getTimeMillis(),
                    record.getValue());

            record = parser.nextDoubleRecord();
        }

        return edv;
    }

    /**
     * Given a list of configurations, it simulates all of them that has
     * double as expected type.
     *
     * @param configs list containing all configurations that have to be tested.
     * @return {@code Map} of key {@code MockDataConfig} and value {@code ExpectedValue}. {@link
     * ExpectedDoubleValue}.
     **/
    public static Map<MockDataConfig, ExpectedValue> simulateSingleton(List<MockDataConfig> configs)
        throws ClassNotFoundException, NoSuchMethodException, IOException, IllegalAccessException,
        InvocationTargetException, InstantiationException {
        Map<MockDataConfig, ExpectedValue> expectedOutput = new HashMap<>();

        for (MockDataConfig config : configs) {
            MockConfigToCsvParser parser = new MockConfigToCsvParser(config);

            if (config.getValueFields() != null && config.getValueFields().size() == 1) {
                expectedOutput.put(config,
                        MockAggregator.simulateSingletonCollector(parser));
            }
        }

        return expectedOutput;
    }

    /**
     * Given a list of configurations, it simulates all of them that has
     * array as expected type.
     *
     * @param configs list containing all configurations that have to be tested.
     * @return {@code Map} of key {@code MockDataConfig} and value {@code ExpectedValue}. {@link
     * ExpectedDoubleValue}.
     **/
    public static Map<MockDataConfig, ExpectedValue> simulateArray(List<MockDataConfig> configs)
        throws ClassNotFoundException, NoSuchMethodException, IOException, IllegalAccessException,
        InvocationTargetException, InstantiationException {
        Map<MockDataConfig, ExpectedValue> exepctedValue = new HashMap<>();

        for (MockDataConfig config : configs) {
            try (MockConfigToCsvParser parser = new MockConfigToCsvParser(config)) {
                if (config.getValueFields() != null && config.getValueFields().size() > 1) {
                    exepctedValue.put(config, MockAggregator.simulateArrayCollector(parser));
                }
            }
        }

        return exepctedValue;
    }

    /**
     * Simulates all possible test case scenarios configured in mock-configuration.
     *
     * @return {@code Map} of key {@code MockDataConfig} and value {@code ExpectedValue}. {@link
     * ExpectedDoubleValue}.
     **/
    public static Map<MockDataConfig, ExpectedValue> getSimulations(
            List<MockDataConfig> mockDataConfigs)
        throws ClassNotFoundException, NoSuchMethodException, IOException, IllegalAccessException,
        InvocationTargetException, InstantiationException {
        Map<MockDataConfig, ExpectedValue> map = new HashMap<>();
        map.putAll(simulateSingleton(mockDataConfigs));
        map.putAll(simulateArray(mockDataConfigs));

        return map;
    }
}
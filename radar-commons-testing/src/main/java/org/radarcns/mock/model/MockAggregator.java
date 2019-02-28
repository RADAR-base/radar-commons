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

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.mock.data.MockCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MockAggregator simulates the behaviour of a Kafka Streams application based on time window.
 * It supported accumulators are <ul>
 * <li>array of {@code Double}
 * <li>singleton {@code Double}
 * </ul>
 */
public class MockAggregator {
    private static final Logger logger = LoggerFactory.getLogger(MockAggregator.class);
    private final List<MockDataConfig> mockDataConfigs;
    private final Path root;

    /**
     * Default constructor.
     */
    public MockAggregator(List<MockDataConfig> mockDataConfigs, Path root) {
        this.mockDataConfigs = mockDataConfigs;
        this.root = root;
    }

    /**
     * Simulates all possible test case scenarios configured in mock-configuration.
     *
     * @return {@code Map} of key {@code MockDataConfig} and value {@code ExpectedValue}. {@link
     * ExpectedDoubleValue}.
     **/
    @SuppressWarnings("unused")
    public Map<MockDataConfig, ExpectedValue> simulate() throws IOException {

        Map<MockDataConfig, ExpectedValue> expectedValue = new HashMap<>();

        for (MockDataConfig config : mockDataConfigs) {
            if (config.getValueFields() == null || config.getValueFields().isEmpty()) {
                logger.warn("No value fields specified for {}. Skipping.", config.getTopic());
                continue;
            }

            try (MockCsvParser<ObservationKey> parser = new MockCsvParser<>(config, root)) {
                Schema valueSchema = config.parseAvroTopic().getValueSchema();
                List<String> valueFields = config.getValueFields();

                ExpectedValue<?> value;
                if (config.getValueFields().size() == 1) {
                    value = new ExpectedDoubleValue(valueSchema, valueFields);
                } else {
                    value = new ExpectedArrayValue(valueSchema, valueFields);
                }

                while (parser.hasNext()) {
                    value.add(parser.next());
                }

                expectedValue.put(config, value);
            }
        }

        return expectedValue;
    }
}
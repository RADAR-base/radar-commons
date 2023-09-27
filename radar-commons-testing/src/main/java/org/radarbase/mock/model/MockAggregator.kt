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
package org.radarbase.mock.model

import com.opencsv.exceptions.CsvValidationException
import org.apache.avro.specific.SpecificRecord
import org.radarbase.mock.config.MockDataConfig
import org.radarbase.mock.data.MockCsvParser
import org.radarbase.producer.schema.SchemaRetriever
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.time.Instant

/**
 * The MockAggregator simulates the behaviour of a Kafka Streams application based on time window.
 * It supported accumulators are
 *  * array of `Double`
 *  * singleton `Double`
 *
 */
class MockAggregator(
    private val mockDataConfigs: List<MockDataConfig>,
    private val root: Path,
    private val retriever: SchemaRetriever,
) {
    /**
     * Simulates all possible test case scenarios configured in mock-configuration.
     *
     * @return `Map` of key `MockDataConfig` and value `ExpectedValue`. [ ].
     */
    @Suppress("unused")
    @Throws(IOException::class)
    fun simulate(): Map<MockDataConfig, ExpectedValue<*>> = buildMap {
        for (config in mockDataConfigs) {
            val valueFields = config.valueFields
            if (valueFields.isNullOrEmpty()) {
                logger.warn("No value fields specified for {}. Skipping.", config.topic)
                continue
            }
            val now = Instant.now()
            try {
                MockCsvParser(config, root, now, retriever).use { parser ->
                    val valueSchema = config.parseAvroTopic<SpecificRecord, SpecificRecord>().valueSchema
                    val value: ExpectedValue<*> = if (valueFields.size == 1) {
                        ExpectedDoubleValue(valueSchema, valueFields)
                    } else {
                        ExpectedArrayValue(valueSchema, valueFields)
                    }
                    while (parser.hasNext()) {
                        value.add(parser.next())
                    }
                    put(config, value)
                }
            } catch (ex: CsvValidationException) {
                throw IOException(ex)
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MockAggregator::class.java)
    }
}

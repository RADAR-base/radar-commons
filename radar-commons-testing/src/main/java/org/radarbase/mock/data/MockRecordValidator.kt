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
package org.radarbase.mock.data

import com.opencsv.exceptions.CsvValidationException
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.radarbase.data.Record
import org.radarbase.mock.config.MockDataConfig
import org.radarbase.producer.schema.SchemaRetriever
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.time.Instant

/**
 * CSV files must be validated before using since MockAggregator can handle only files containing
 * unique User_ID and Source_ID and having increasing timestamp at each raw.
 */
class MockRecordValidator(
    private val config: MockDataConfig, private val duration: Long, private val root: Path,
    private val retriever: SchemaRetriever
) {
    private var timePos = 0
    private var time: Double
    private var startTime: Double

    /** Create a new validator for given configuration.  */
    init {
        time = Double.NaN
        startTime = Double.NaN
    }

    /**
     * Verify whether the CSV file can be used or not.
     * @throws IllegalArgumentException if the CSV file does not respect the constraints.
     */
    suspend fun validate() {
        val now = Instant.now()
        try {
            MockCsvParser(config, root, now, retriever).use { parser ->
                parser.initialize()
                require(parser.hasNext()) { "CSV file is empty" }
                val valueSchema =
                    config.parseAvroTopic<SpecificRecord, SpecificRecord>().valueSchema
                var timeField = valueSchema.getField("timeReceived")
                if (timeField == null) {
                    timeField = valueSchema.getField("time")
                }
                timePos = timeField!!.pos()
                var last: Record<GenericRecord, GenericRecord>? = null
                var line = 1L
                while (parser.hasNext()) {
                    val record = parser.next()
                    checkRecord(record, last, line++)
                    last = record
                }
                checkDuration()
                checkFrequency(line)
            }
        } catch (e: IOException) {
            error("Cannot open file", -1, e)
        } catch (e: CsvValidationException) {
            error("Cannot open file", -1, e)
        }
    }

    private fun checkFrequency(line: Long) {
        val expected = config.frequency * duration / 1000L + 1L
        if (line != config.frequency * duration / 1000L + 1L) {
            error("CSV contains fewer messages $line than expected $expected", -1L, null)
        }
    }

    private fun checkRecord(
        record: Record<GenericRecord, GenericRecord>,
        last: Record<GenericRecord, GenericRecord>?,
        line: Long
    ) {
        val previousTime = time
        time = record.value[timePos] as Double
        if (last == null) {
            // no checks, only update initial time stamp
            startTime = time
        } else if (last.key != record.key) {
            error("It is possible to test only one user/source at time.", line, null)
        } else if (time < previousTime) {
            error("Time must increase row by row.", line, null)
        }
    }

    private fun error(message: String, line: Long, ex: Exception?) {
        val messageBuilder = StringBuilder(150)
        messageBuilder
            .append(config.dataFile)
            .append(" with topic ")
            .append(config.topic)
            .append(" is invalid")
        if (line > 0L) {
            messageBuilder
                .append(" on line ")
                .append(line)
        }
        val fullMessage = messageBuilder
            .append(". ")
            .append(message)
            .toString()
        logger.error(fullMessage)
        throw IllegalArgumentException(fullMessage, ex)
    }

    private fun checkDuration() {
        val interval = (time * 1000.0).toLong() - (startTime * 1000.0).toLong()

        // add a margin of 50 for clock error purposes
        val margin = 50L
        if (duration <= interval - margin || duration > interval + 1000L + margin) {
            error(
                "Data does not cover " + duration + " milliseconds but "
                        + interval + " instead.", -1L, null
            )
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(
            MockRecordValidator::class.java
        )
    }
}

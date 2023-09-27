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
package org.radarbase.mock

import com.opencsv.CSVReader
import com.opencsv.exceptions.CsvValidationException
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.radarbase.mock.config.MockDataConfig
import org.radarbase.mock.data.CsvGenerator
import org.radarbase.mock.data.MockRecordValidatorTest
import org.radarbase.mock.data.RecordGenerator
import org.radarcns.kafka.ObservationKey
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.bufferedReader

class CsvGeneratorTest {
    @Throws(IOException::class)
    private fun makeConfig(folder: Path): MockDataConfig {
        return MockRecordValidatorTest.makeConfig(folder)
    }

    @Test
    @Throws(IOException::class, CsvValidationException::class)
    fun generateMockConfig(@TempDir folder: Path) {
        val generator = CsvGenerator()
        val config = makeConfig(folder)
        generator.generate(config, 100000L, folder.root)
        val p = Paths.get(checkNotNull(config.dataFile))
        p.bufferedReader().use { reader ->
            CSVReader(reader).use { parser ->
                val headers = arrayOf(
                    "key.projectId",
                    "key.userId",
                    "key.sourceId",
                    "value.time",
                    "value.timeReceived",
                    "value.light",
                )
                assertArrayEquals(headers, parser.readNext())
                val n = generateSequence { parser.readNext() }
                    .map { line ->
                        val value = line[5]
                        assertNotEquals("NaN", value)
                        assertNotEquals("Infinity", value)
                        assertNotEquals("-Infinity", value)
                        // no decimals lost or appended
                        assertEquals(value, value.toFloat().toString())
                    }
                    .count()

                assertEquals(100, n)
            }
        }
    }

    @Test
    @Throws(IOException::class, CsvValidationException::class)
    fun generateGenerator(@TempDir folder: Path) {
        val generator = CsvGenerator()
        val config = makeConfig(folder)
        val time = (System.currentTimeMillis() / 1000.0).toString()
        val recordGenerator: RecordGenerator<ObservationKey> =
            object : RecordGenerator<ObservationKey>(
                config,
                ObservationKey::class.java,
            ) {
                override fun iteratableRawValues(
                    key: ObservationKey,
                    duration: Long,
                ): Iterable<Array<String>> = listOf(
                    arrayOf("test", "UserID_0", "SourceID_0", time, time, 0.12311241241042352.toFloat().toString()),
                )
            }
        val p = Paths.get(checkNotNull(config.dataFile))
        generator.generate(recordGenerator, 1000L, p)
        p.bufferedReader().use { reader ->
            CSVReader(reader).use { parser ->
                assertArrayEquals(
                    recordGenerator.headerArray,
                    parser.readNext(),
                )
                // float will cut off a lot of decimals
                assertArrayEquals(
                    arrayOf("test", "UserID_0", "SourceID_0", time, time, "0.12311241"),
                    parser.readNext(),
                )
            }
        }
    }
}

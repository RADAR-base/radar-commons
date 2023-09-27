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

import com.opencsv.CSVWriter
import org.radarbase.mock.config.MockDataConfig
import org.radarcns.kafka.ObservationKey
import java.io.IOException
import java.nio.file.Path
import kotlin.io.path.bufferedWriter

/**
 * It generates a CVS file that can be used to stream data and
 * to compute the expected results.
 * @param key record key, project test, user UserID_0 and source SourceID_0 by default.
 */
class CsvGenerator(
    private val key: ObservationKey = ObservationKey("test", "UserID_0", "SourceID_0"),
) {
    /** CsvGenerator sending data with given key.  */
    /**
     * Generates new CSV file to simulation a single user with a single device.
     *
     * @param config properties containing metadata to generate data
     * @param duration simulation duration expressed in milliseconds
     * @param root directory relative to which the output csv file is generated
     * @throws IOException if the CSV file cannot be written to
     */
    @Throws(IOException::class)
    fun generate(config: MockDataConfig, duration: Long, root: Path) {
        val file = config.getDataFile(root)
        generate(RecordGenerator(config, ObservationKey::class.java), duration, file)
    }

    /**
     * Generates new CSV file to simulation a single user with a single device.
     *
     * @param generator generator to generate data
     * @param duration simulation duration expressed in milliseconds
     * @param csvFile CSV file to write data to
     * @throws IOException if the CSV file cannot be written to
     */
    @Throws(IOException::class)
    fun generate(generator: RecordGenerator<ObservationKey>, duration: Long, csvFile: Path) {
        csvFile.bufferedWriter().use { writer ->
            CSVWriter(writer).use { csvWriter ->
                csvWriter.writeNext(generator.headerArray)
                csvWriter.writeAll(generator.iteratableRawValues(key, duration))
            }
        }
    }
}

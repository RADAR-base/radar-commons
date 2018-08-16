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

package org.radarcns.mock.data;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.util.CsvWriter;

/**
 * It generates a CVS file that can be used to stream data and
 * to compute the expected results.
 */
public final class CsvGenerator {
    private final ObservationKey key;

    /** CsvGenerator sending data as project test, user UserID_0 and source SourceID_0. */
    public CsvGenerator() {
        this(new ObservationKey("test", "UserID_0", "SourceID_0"));
    }

    /** CsvGenerator sending data with given key. */
    public CsvGenerator(ObservationKey key) {
        this.key = key;
    }

    /**
     * Generates new CSV file to simulation a single user with a single device.
     *
     * @param config properties containing metadata to generate data
     * @param duration simulation duration expressed in milliseconds
     * @param root directory relative to which the output csv file is generated
     * @throws IOException if the CSV file cannot be written to
     */
    public void generate(MockDataConfig config, long duration, File root)
            throws IOException {
        File file = config.getDataFile(root);

        try {
            generate(new RecordGenerator<>(config, ObservationKey.class), duration, file);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException
                | ClassNotFoundException ex) {
            throw new IOException("Failed to generate data", ex);
        }
    }

    /**
     * Generates new CSV file to simulation a single user with a single device.
     *
     * @param generator generator to generate data
     * @param duration simulation duration expressed in milliseconds
     * @param csvFile CSV file to write data to
     * @throws IOException if the CSV file cannot be written to
     */
    public void generate(RecordGenerator<ObservationKey> generator, long duration, File csvFile)
            throws IOException {
        try (CsvWriter writer = new CsvWriter(csvFile, generator.getHeader())) {
            writer.writeRows(generator.iterateRawValues(key, duration));
        }
    }
}

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

package org.radarcns.mock;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.radarcns.key.MeasurementKey;
import org.radarcns.util.CsvWriter;

/**
 * It generates a CVS file that can be used to stream data and
 * to compute the expected results.
 */
public final class CsvGenerator {
    /**
     * Generates new CSV file to simulation a single user with a single device as longs as seconds.
     *
     * @param config properties containing metadata to generate data
     * @param duration simulation duration expressed in seconds
     * @param root directory relative to which the output csv file is generated
     * @throws IOException if the CSV file cannot be written to
     */
    public void generate(MockDataConfig config, long duration, File root)
            throws IOException {
        File file = config.getDataFile(root);

        try {
            generate(new RecordGenerator<>(config, MeasurementKey.class), duration, file);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException
                | ClassNotFoundException ex) {
            throw new IOException("Failed to generate data", ex);
        }
    }

    /**
     * Generates new CSV file to simulation a single user with a single device as longs as seconds.
     *
     * @param generator generator to generate data
     * @param duration simulation duration expressed in seconds
     * @param csvFile CSV file to write data to
     * @throws IOException if the CSV file cannot be written to
     */
    public void generate(RecordGenerator<MeasurementKey> generator, long duration, File csvFile)
            throws IOException {
        MeasurementKey key = new MeasurementKey("UserID_0", "SourceID_0");

        try (CsvWriter writer = new CsvWriter(csvFile, generator.getHeader())) {
            writer.writeRows(generator.iterateRawValues(key, duration));
        }
    }
}

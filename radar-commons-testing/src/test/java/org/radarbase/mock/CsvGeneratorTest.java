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

package org.radarbase.mock;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.radarbase.mock.config.MockDataConfig;
import org.radarbase.mock.data.CsvGenerator;
import org.radarbase.mock.data.MockRecordValidatorTest;
import org.radarbase.mock.data.RecordGenerator;
import org.radarcns.kafka.ObservationKey;

public class CsvGeneratorTest {
        private MockDataConfig makeConfig(Path folder) throws IOException {
        return MockRecordValidatorTest.makeConfig(folder);
    }

    @Test
    public void generateMockConfig(@TempDir Path folder) throws IOException, CsvValidationException {
        CsvGenerator generator = new CsvGenerator();

        MockDataConfig config = makeConfig(folder);
        generator.generate(config, 100_000L, folder.getRoot());

        Path p = Paths.get(config.dataFile);
        try (Reader reader = Files.newBufferedReader(p);
                CSVReader parser = new CSVReader(reader)) {
            String[] headers = {"key.projectId", "key.userId", "key.sourceId", "value.time", "value.timeReceived", "value.light"};
            assertArrayEquals(headers, parser.readNext());

            int n = 0;
            String[] line;
            while ((line = parser.readNext()) != null) {
                String value = line[5];
                assertNotEquals("NaN", value);
                assertNotEquals("Infinity", value);
                assertNotEquals("-Infinity", value);
                // no decimals lost or appended
                assertEquals(value, Float.valueOf(value).toString());
                n++;
            }
            assertEquals(100, n);
        }
    }

    @Test
    public void generateGenerator(@TempDir Path folder) throws IOException, CsvValidationException {
        CsvGenerator generator = new CsvGenerator();

        MockDataConfig config = makeConfig(folder);

        final String time = Double.toString(System.currentTimeMillis() / 1000d);

        RecordGenerator<ObservationKey> recordGenerator = new RecordGenerator<ObservationKey>(
                config, ObservationKey.class) {

            @Override
            public Iterable<String[]> iteratableRawValues(ObservationKey key, long duration) {
                return List.<String[]>of(new String[] {
                        "test", "UserID_0", "SourceID_0", time, time,
                        Float.valueOf((float)0.123112412410423518).toString()
                });
            }
        };

        generator.generate(recordGenerator, 1000L, Paths.get(config.dataFile));

        Path p = Paths.get(config.dataFile);

        try (Reader reader = Files.newBufferedReader(p);
                CSVReader parser = new CSVReader(reader)) {
            assertArrayEquals(
                    recordGenerator.getHeader().toArray(new String[0]),
                    parser.readNext());
            // float will cut off a lot of decimals
            assertArrayEquals(
                    new String[] { "test", "UserID_0", "SourceID_0", time, time, "0.12311241" },
                    parser.readNext());
        }
    }
}

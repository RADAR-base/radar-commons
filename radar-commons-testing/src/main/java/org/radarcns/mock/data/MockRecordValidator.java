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

import java.io.IOException;
import java.nio.file.Path;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.mock.config.MockDataConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSV files must be validate before using since MockAggregator can handle only files containing
 *      unique User_ID and Source_ID and having increasing timestamp at each raw.
 */
public class MockRecordValidator {
    private static final Logger logger = LoggerFactory.getLogger(MockRecordValidator.class);
    private final MockDataConfig config;
    private final long duration;
    private final Path root;
    private int timePos;
    private double time;
    private double startTime;

    /** Create a new validator for given configuration. */
    public MockRecordValidator(MockDataConfig config, long duration, Path root) {
        this.config = config;
        this.duration = duration;
        this.root = root;
        this.time = Double.NaN;
        this.startTime = Double.NaN;
    }

    /**
     * Verify whether the CSV file can be used or not.
     * @throws IllegalArgumentException if the CSV file does not respect the constraints.
     */
    public void validate() {
        try (MockCsvParser<ObservationKey> parser = new MockCsvParser<>(config, root)) {
            if (!parser.hasNext()) {
                throw new IllegalArgumentException("CSV file is empty");
            }

            timePos = config.parseAvroTopic().getValueSchema()
                    .getField("timeReceived").pos();

            Record<ObservationKey, SpecificRecord> last = null;
            long line = 1L;

            while (parser.hasNext()) {
                Record<ObservationKey, SpecificRecord> record = parser.next();
                checkRecord(record, last, line++);
                last = record;
            }

            checkDuration();
            checkFrequency(line);
        } catch (IOException e) {
            error("Cannot open file", -1, e);
        }
    }

    private void checkFrequency(long line) {
        if (line != config.getFrequency() * duration / 1000L + 1L) {
            error("CSV contains fewer messages than expected.", -1L, null);
        }
    }

    private void checkRecord(Record<ObservationKey, SpecificRecord> record,
            Record<ObservationKey, SpecificRecord> last, long line) {
        double previousTime = time;
        time = (Double) record.value.get(timePos);

        if (last == null) {
            // no checks, only update initial time stamp
            startTime = time;
        } else if (!last.key.equals(record.key)) {
            error("It is possible to test only one user/source at time.", line, null);
        } else if (time < previousTime) {
            error("Time must increase row by row.", line, null);
        }
    }

    private void error(String message, long line, Exception ex) {
        StringBuilder messageBuilder = new StringBuilder(150);
        messageBuilder
                .append(config.getDataFile())
                .append(" with topic ")
                .append(config.getTopic())
                .append(" is invalid");
        if (line > 0L) {
            messageBuilder
                    .append(" on line ")
                    .append(line);
        }
        String fullMessage = messageBuilder
                .append(". ")
                .append(message)
                .toString();
        logger.error(fullMessage);
        throw new IllegalArgumentException(fullMessage, ex);
    }

    private void checkDuration() {
        long interval = (long)(time * 1000d) - (long)(startTime * 1000d);

        // add a margin of 50 for clock error purposes
        long margin = 50L;

        if (duration <= interval - margin || duration > interval + 1000L + margin) {
            error("Data does not cover " + duration + " milliseconds but "
                    + interval + " instead.", -1L, null);
        }
    }
}

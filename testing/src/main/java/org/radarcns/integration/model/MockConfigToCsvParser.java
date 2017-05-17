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

package org.radarcns.integration.model;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.key.MeasurementKey;
import org.radarcns.mock.MockDataConfig;
import org.radarcns.mock.MockFile;

/**
 * Starting from a CSV file, this parser generates a map containing all available fields.
 * The value field can contain either a Double or an array of Doubles.
 */
public class MockConfigToCsvParser implements Closeable {
    private final MockFile<MeasurementKey> mockFile;
    private final int timeReceivedPos;
    private final int[] valuePos;

    /**
     * Constructor that initialises the {@code CSVReader} and computes the {@code ExpectedType}.
     *
     * @param config containing the CSV file path that has to be parsed
     **/
    public MockConfigToCsvParser(MockDataConfig config)
        throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
        IllegalAccessException, IOException {
        this.mockFile = new MockFile<>(new File(config.getAbsoluteDataFile()), config);
        Schema valueSchema = mockFile.getTopic().getValueSchema();
        timeReceivedPos = valueSchema.getField("timeReceived").pos();

        valuePos = new int[config.getValueFields().size()];
        for (int i = 0; i < valuePos.length; i++) {
            valuePos[i] = valueSchema.getField(config.getValueFields().get(i)).pos();
        }
    }

    /**
     * Record computed by the next available
     * raw of CSV file.
     **/
    public Record<MeasurementKey, SpecificRecord> next() throws IOException {
        if (!mockFile.hasNext()) {
            return null;
        }
        return mockFile.next();
    }

    /**
     * Double type Record computed by the next available
     * raw of CSV file.
     */
    public MockRecord.DoubleType nextDoubleRecord() throws IOException {
        Record<MeasurementKey, SpecificRecord> record = next();
        if (record == null) {
            return null;
        }
        MockRecord.DoubleType result = new MockRecord.DoubleType();
        result.setKey(record.key);
        result.setTime((Double)record.value.get(timeReceivedPos));
        result.setValue(((Number)record.value.get(valuePos[0])).doubleValue());
        return result;
    }

    /**
     * Double array type Record computed by the next available
     * raw of CSV file.
     */
    public MockRecord.DoubleArrayType nextDoubleArrayRecord() throws IOException {
        Record<MeasurementKey, SpecificRecord> record = next();
        if (record == null) {
            return null;
        }
        MockRecord.DoubleArrayType result = new MockRecord.DoubleArrayType();
        result.setKey(record.key);
        result.setTime((Double)record.value.get(timeReceivedPos));
        double[] values = new double[valuePos.length];
        for (int i = 0; i < valuePos.length; i++) {
            values[i] = ((Number)record.value.get(valuePos[i])).doubleValue();
        }
        result.setValues(values);
        return result;
    }

    /**
     * Close the {@code MockConfigToCsvParser} closing the CSV reader.
     **/
    public void close() throws IOException {
        mockFile.close();
    }
}
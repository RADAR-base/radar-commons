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

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.phone.PhoneAcceleration;
import org.radarcns.phone.PhoneLight;

public class MockRecordValidatorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private MockDataConfig makeConfig() throws IOException {
        MockDataConfig config = new MockDataConfig();
        config.setDataFile(folder.newFile().getAbsolutePath());
        config.setValueSchema(PhoneLight.class.getName());
        config.setValueField("light");
        config.setTopic("test");
        return config;
    }

    @Test
    public void validate() throws Exception {
        CsvGenerator generator = new CsvGenerator();

        MockDataConfig config = makeConfig();
        generator.generate(config, 100_000L, folder.getRoot());

        // doesn't throw
        new MockRecordValidator(config, 100_000L, folder.getRoot()).validate();
    }

    @Test
    public void validateWrongDuration() throws Exception {
        CsvGenerator generator = new CsvGenerator();

        MockDataConfig config = makeConfig();
        generator.generate(config, 100_000L, folder.getRoot());

        exception.expect(IllegalArgumentException.class);
        new MockRecordValidator(config, 10_000L, folder.getRoot()).validate();
    }

    @Test
    public void validateCustom() throws Exception {
        MockDataConfig config = makeConfig();

        try (FileWriter writer = new FileWriter(config.getDataFile(folder.getRoot()))) {
            writer.append("userId,sourceId,time,timeReceived,light\n");
            writer.append("a,b,1,1,1\n");
            writer.append("a,b,1,2,1\n");
        }

        new MockRecordValidator(config, 2_000L, folder.getRoot()).validate();
    }

    @Test
    public void validateWrongKey() throws Exception {
        MockDataConfig config = makeConfig();

        try (FileWriter writer = new FileWriter(config.getDataFile(folder.getRoot()))) {
            writer.append("userId,sourceId,time,timeReceived,light\n");
            writer.append("a,b,1,1,1\n");
            writer.append("a,c,1,2,1\n");
        }

        exception.expect(IllegalArgumentException.class);
        new MockRecordValidator(config, 2_000L, folder.getRoot()).validate();
    }

    @Test
    public void validateWrongTime() throws Exception {
        MockDataConfig config = makeConfig();

        try (FileWriter writer = new FileWriter(config.getDataFile(folder.getRoot()))) {
            writer.append("userId,sourceId,time,timeReceived,light\n");
            writer.append("a,b,1,1,1\n");
            writer.append("a,b,1,0,1\n");
        }

        exception.expect(IllegalArgumentException.class);
        new MockRecordValidator(config, 2_000L, folder.getRoot()).validate();
    }


    @Test
    public void validateMissingKeyField() throws Exception {
        MockDataConfig config = makeConfig();

        try (FileWriter writer = new FileWriter(config.getDataFile(folder.getRoot()))) {
            writer.append("userId,time,timeReceived,light\n");
            writer.append("a,1,1,1\n");
            writer.append("a,1,2,1\n");
        }

        exception.expect(NullPointerException.class);
        new MockRecordValidator(config, 2_000L, folder.getRoot()).validate();
    }

    @Test
    public void validateMissingValueField() throws Exception {
        MockDataConfig config = makeConfig();

        try (FileWriter writer = new FileWriter(config.getDataFile(folder.getRoot()))) {
            writer.append("userId,sourceId,time,light\n");
            writer.append("a,b,1,1\n");
            writer.append("a,b,1,2\n");
        }

        exception.expect(NullPointerException.class);
        new MockRecordValidator(config, 2_000L, folder.getRoot()).validate();
    }

    @Test
    public void validateMissingValue() throws Exception {
        MockDataConfig config = makeConfig();

        try (FileWriter writer = new FileWriter(config.getDataFile(folder.getRoot()))) {
            writer.append("userId,sourceId,time,timeReceived,light\n");
            writer.append("a,b,1,1\n");
            writer.append("a,b,1,2,1\n");
        }

        exception.expect(IllegalArgumentException.class);
        new MockRecordValidator(config, 2_000L, folder.getRoot()).validate();
    }

    @Test
    public void validateWrongValueType() throws Exception {
        MockDataConfig config = makeConfig();

        try (FileWriter writer = new FileWriter(config.getDataFile(folder.getRoot()))) {
            writer.append("userId,sourceId,time,timeReceived,light\n");
            writer.append("a,b,1,1,a\n");
            writer.append("a,b,1,2,b\n");
        }

        exception.expect(NumberFormatException.class);
        new MockRecordValidator(config, 2_000L, folder.getRoot()).validate();
    }

    @Test
    public void validateMultipleFields() throws Exception {
        MockDataConfig config = makeConfig();
        config.setValueSchema(PhoneAcceleration.class.getName());
        config.setValueFields(Arrays.asList("x", "y", "z"));

        try (FileWriter writer = new FileWriter(config.getDataFile(folder.getRoot()))) {
            writer.append("userId,sourceId,time,timeReceived,x,y,z\n");
            writer.append("a,b,1,1,1,1,1\n");
            writer.append("a,b,1,2,1,1,1\n");
        }

        new MockRecordValidator(config, 2_000L, folder.getRoot()).validate();
    }
}
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

package org.radarbase.mock.data;

import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.radarbase.mock.config.MockDataConfig;
import org.radarcns.passive.phone.PhoneAcceleration;
import org.radarcns.passive.phone.PhoneLight;

public class MockRecordValidatorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root;

    private MockDataConfig makeConfig() throws IOException {
        return makeConfig(folder);
    }

    @Before
    public void setUp() {
        root = folder.getRoot().toPath();
    }

    public static MockDataConfig makeConfig(TemporaryFolder folder) throws IOException {
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
        generator.generate(config, 100_000L, root);

        new MockRecordValidator(config, 100_000L, root).validate();
    }

    @Test
    public void validateWrongDuration() throws Exception {
        CsvGenerator generator = new CsvGenerator();

        MockDataConfig config = makeConfig();
        generator.generate(config, 100_000L, root);

        assertValidateThrows(IllegalArgumentException.class, config);
    }

    @Test
    public void validateCustom() throws Exception {
        MockDataConfig config = makeConfig();

        try (Writer writer = Files.newBufferedWriter(config.getDataFile(root))) {
            writer.append("projectId,userId,sourceId,time,timeReceived,light\n");
            writer.append("test,a,b,1,1,1\n");
            writer.append("test,a,b,1,2,1\n");
        }

        assertValidate(config);
    }

    @Test
    public void validateWrongKey() throws Exception {
        MockDataConfig config = makeConfig();

        try (Writer writer = Files.newBufferedWriter(config.getDataFile(root))) {
            writer.append("projectId,userId,sourceId,time,timeReceived,light\n");
            writer.append("test,a,b,1,1,1\n");
            writer.append("test,a,c,1,2,1\n");
        }

        assertValidateThrows(IllegalArgumentException.class, config);
    }

    @Test
    public void validateWrongTime() throws Exception {
        MockDataConfig config = makeConfig();

        try (Writer writer = Files.newBufferedWriter(config.getDataFile(root))) {
            writer.append("projectId,userId,sourceId,time,timeReceived,light\n");
            writer.append("test,a,b,1,1,1\n");
            writer.append("test,a,b,1,0,1\n");
        }

        assertValidateThrows(IllegalArgumentException.class, config);
    }


    @Test
    public void validateMissingKeyField() throws Exception {
        MockDataConfig config = makeConfig();

        try (Writer writer = Files.newBufferedWriter(config.getDataFile(root))) {
            writer.append("projectId,userId,time,timeReceived,light\n");
            writer.append("test,a,1,1,1\n");
            writer.append("test,a,1,2,1\n");
        }

        assertValidateThrows(NullPointerException.class, config);
    }

    @Test
    public void validateMissingValueField() throws Exception {
        MockDataConfig config = makeConfig();

        try (Writer writer = Files.newBufferedWriter(config.getDataFile(root))) {
            writer.append("projectId,userId,sourceId,time,light\n");
            writer.append("test,a,b,1,1\n");
            writer.append("test,a,b,1,2\n");
        }

        assertValidateThrows(NullPointerException.class, config);
    }

    @Test
    public void validateMissingValue() throws Exception {
        MockDataConfig config = makeConfig();

        try (Writer writer = Files.newBufferedWriter(config.getDataFile(root))) {
            writer.append("projectId,userId,sourceId,time,timeReceived,light\n");
            writer.append("test,a,b,1,1\n");
            writer.append("test,a,b,1,2,1\n");
        }

        assertValidateThrows(ArrayIndexOutOfBoundsException.class, config);
    }

    @Test
    public void validateWrongValueType() throws Exception {
        MockDataConfig config = makeConfig();

        try (Writer writer = Files.newBufferedWriter(config.getDataFile(root))) {
            writer.append("projectId,userId,sourceId,time,timeReceived,light\n");
            writer.append("test,a,b,1,1,a\n");
            writer.append("test,a,b,1,2,b\n");
        }

        assertValidateThrows(NumberFormatException.class, config);
    }

    @Test
    public void validateMultipleFields() throws Exception {
        MockDataConfig config = makeConfig();
        config.setValueSchema(PhoneAcceleration.class.getName());
        config.setValueFields(Arrays.asList("x", "y", "z"));

        try (Writer writer = Files.newBufferedWriter(config.getDataFile(root))) {
            writer.append("projectId,userId,sourceId,time,timeReceived,x,y,z\n");
            writer.append("test,a,b,1,1,1,1,1\n");
            writer.append("test,a,b,1,2,1,1,1\n");
        }

        assertValidate(config);
    }

    private <T extends Throwable> void assertValidateThrows(Class<T> ex, MockDataConfig config) {
        MockRecordValidator validator = new MockRecordValidator(config, 2_000L, root);
        assertThrows(ex, validator::validate);
    }

    private void assertValidate(MockDataConfig config) {
        new MockRecordValidator(config, 2_000L, root).validate();
    }
}

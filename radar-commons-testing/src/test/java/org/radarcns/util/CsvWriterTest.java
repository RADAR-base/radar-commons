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

package org.radarcns.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class CsvWriterTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void write() throws Exception {
        File f = folder.newFile();
        try (CsvWriter writer = new CsvWriter(f, Arrays.asList("a", "b"))) {
            writer.writeRows(Arrays.asList(
                    Arrays.asList("1", "2"),
                    Arrays.asList("3", "4")).iterator());
        }

        String result = new String(Files.readAllBytes(f.toPath()));
        assertThat(result, is(equalTo("a,b\n1,2\n3,4\n")));

        try (FileReader fr = new FileReader(f);
                BufferedReader reader = new BufferedReader(fr)) {
            CsvParser parser = new CsvParser(reader);
            assertThat(parser.parseLine(), contains("a", "b"));
            assertThat(parser.parseLine(), contains("1", "2"));
            assertThat(parser.parseLine(), contains("3", "4"));
            assertThat(parser.parseLine(), is(nullValue()));
        }
    }
}
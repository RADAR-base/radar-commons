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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

/**
 * CSV writer.
 */
public class CsvWriter {
    public static final char DEFAULT_SEPARATOR = ',';
    private final int separator;

    public CsvWriter() {
        this(DEFAULT_SEPARATOR);
    }

    public CsvWriter(char separator) {
        this.separator = separator;
    }

    /**
     * Writes a CSV file.
     *
     * @param header header to write
     * @param data data to write, with each iteration yielding the same size elements
     * @param file that has to be written
     **/
    public void write(File file, List<String> header, Iterator<List<String>> data)
            throws IOException {

        try (FileWriter fw = new FileWriter(file, false);
                BufferedWriter writer = new BufferedWriter(fw)) {
            write(writer, header, data);
        }
    }

    /**
     * Writes a CSV file.
     *
     * @param header header to write
     * @param data data to write, with each iteration yielding the same size elements
     * @param writer sink to write to
     **/
    public void write(Writer writer, List<String> header, Iterator<List<String>> data)
            throws IOException {
        writeRow(header, writer);

        while (data.hasNext()) {
            writeRow(data.next(), writer);
        }
    }

    private void writeRow(List<String> strings, Writer writer) throws IOException {
        boolean first = true;
        for (String v : strings) {
            if (first) {
                first = false;
            } else {
                writer.write(separator);
            }
            writer.write(v);
        }
        writer.write('\n');
    }
}

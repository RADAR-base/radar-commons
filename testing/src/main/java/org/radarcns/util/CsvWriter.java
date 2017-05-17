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
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.Flushable;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

/**
 * CSV writer.
 */
public class CsvWriter implements Flushable, Closeable {
    public static final char DEFAULT_SEPARATOR = ',';
    private final int separator;
    private final boolean shouldClose;
    private final Writer writer;
    private final int headerLength;

    /**
     * Constructs a CSV writer to write to a file. The header is written immediately.
     * @param file file to write to, it will be created or emptied.
     * @param header header to write
     * @throws IOException if writing to the file fails
     */
    public CsvWriter(File file, List<String> header) throws IOException {
        this(new BufferedWriter(new FileWriter(file, false)), header, true, DEFAULT_SEPARATOR);
    }

    /**
     * Constructs a CSV writer to write to a writer. The header is written immediately.
     * @param writer writer to write to, it will not be closed when this CsvWriter is closed.
     * @param header header to write
     * @throws IOException if writing to the writer fails
     */
    public CsvWriter(Writer writer, List<String> header) throws IOException {
        this(writer, header, false, DEFAULT_SEPARATOR);
    }

    /**
     * Constructs a CSV writer to write to a writer. The header is written immediately.
     * @param writer writer to write to
     * @param header header to write
     * @param shouldClose if and only if true, given writer will be closed when this writer is
     *                    closed
     * @param separator separator to use in the CSV file, comma ',' by default.
     * @throws IOException if writing to the writer fails
     */
    public CsvWriter(Writer writer, List<String> header, boolean shouldClose, char separator)
            throws IOException {
        this.separator = separator;
        this.writer = writer;
        this.shouldClose = shouldClose;
        headerLength = header.size();
        writeRow(header);
    }

    /** Write all rows in given iterator. */
    public void writeRows(Iterator<List<String>> rows) throws IOException {
        while (rows.hasNext()) {
            writeRow(rows.next());
        }
    }

    /** Write a single row, with each list entry as a column. */
    public void writeRow(List<String> strings) throws IOException {
        if (strings.size() != headerLength) {
            throw new IllegalArgumentException("Row size does not match header size");
        }
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

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
        if (shouldClose) {
            try (Writer localWriter = writer) {
                localWriter.flush();
            }
        }
    }
}

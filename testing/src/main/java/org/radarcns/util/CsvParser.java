/*
 * Copyright 2017 Kings College London and The Hyve
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

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a CSV file.
 */
public class CsvParser implements Closeable {
    private static final char DEFAULT_SEPARATOR = ',';
    private static final char DEFAULT_QUOTE = '"';

    private final char separator;
    private final char quote;
    private final Reader reader;
    private int length;
    private int row;

    /** CsvParser from reader with double quotes '"' as quotes and comma ',' as separator. */
    public CsvParser(Reader r) {
        this(r, DEFAULT_SEPARATOR, DEFAULT_QUOTE);
    }

    /** CsvParser from reader. */
    public CsvParser(Reader r, char separator, char quote) {
        this.reader = r;
        this.separator = separator;
        this.quote = quote;
        this.length = -1;
        this.row = 0;
    }

    /**
     * Returns a null when the input stream is empty.
     */
    public List<String> parseLine() throws IOException {
        int ch = reader.read();
        while (ch == '\r') {
            ch = reader.read();
        }
        if (ch < 0) {
            return null;
        }
        this.row++;

        List<String> store = new ArrayList<>(length >= 0 ? length : 10);
        ParsingState state = new ParsingState();
        StringBuilder builder = new StringBuilder();

        while (ch >= 0) {
            if (state.inQuotes) {
                processQuoted(ch, state, builder);
            } else if (!processUnquoted(ch, state, builder, store)) {
                break;
            }
            ch = reader.read();
        }
        store.add(builder.toString());

        verifyLength(store);

        return store;
    }

    private void verifyLength(List<String> store) {
        if (length == -1) {
            length = store.size();
        } else if (length != store.size()) {
            throw new IllegalArgumentException("CSV file does not have a fixed number of columns: "
                    + "row " + row + " contains " + store.size()
                    + " columns, instead of " + length + ". Row:\n\t" + store);
        }
    }

    private void processQuoted(int ch, ParsingState state, StringBuilder builder) {
        state.inValue = true;
        if (ch == quote) {
            state.inQuotes = false;
        } else {
            builder.append((char) ch);
        }
    }

    private boolean processUnquoted(int ch, ParsingState state, StringBuilder builder,
            List<String> store) {
        if (ch == quote) {
            state.inQuotes = true;
            if (state.inValue) {
                // this is for the double quote in the middle of a value
                builder.append(quote);
            }
        } else if (ch == separator) {
            store.add(builder.toString());
            builder.setLength(0);
            state.reset();
        } else if (ch == '\n') {
            //end of a line, break out
            return false;
        } else if (ch != '\r') {  //ignore LF characters
            builder.append((char) ch);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static class ParsingState {
        private boolean inQuotes;
        private boolean inValue;

        private void reset() {
            inValue = false;
            inQuotes = false;
        }
    }
}

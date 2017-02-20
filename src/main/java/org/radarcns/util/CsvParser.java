package org.radarcns.util;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses a CSV file
 */
public class CsvParser {
    private static final char DEFAULT_SEPARATOR = ',';
    private static final char DEFAULT_QUOTE = '"';

    private final char separator;
    private final char quote;
    private final Reader reader;
    private int length;
    private int row;

    public CsvParser(Reader r) {
        this(r, DEFAULT_SEPARATOR, DEFAULT_QUOTE);
    }

    public CsvParser(Reader r, char separator, char quote) {
        this.reader = r;
        this.separator = separator;
        this.quote = quote;
        this.length = -1;
        this.row = 0;
    }

    /**
     * Returns a null when the input stream is empty
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
        List<String> store = new ArrayList<>();
        StringBuffer curVal = new StringBuffer();
        boolean inQuotes = false;
        boolean started = false;

        while (ch >= 0) {
            if (inQuotes) {
                started = true;
                if (ch == quote) {
                    inQuotes = false;
                }
                else {
                    curVal.append((char)ch);
                }
            } else {
                if (ch == quote) {
                    inQuotes = true;
                    if (started) {
                        // if this is the second quote in a value, add a quote
                        // this is for the double quote in the middle of a value
                        curVal.append(quote);
                    }
                } else if (ch == separator) {
                    store.add(curVal.toString());
                    curVal = new StringBuffer();
                    started = false;
                } else if (ch == '\n') {
                    //end of a line, break out
                    break;
                } else if (ch != '\r') {  //ignore LF characters
                    curVal.append((char)ch);
                }
            }
            ch = reader.read();
        }
        store.add(curVal.toString());

        if (length == -1) {
            length = store.size();
        } else if (length != store.size()) {
            throw new IllegalArgumentException("CSV file does not have a fixed number of columns: "
                    + "row " + row + " contains " + store.size()
                    + " columns, instead of " + length + ". Row:\n\t" + store);
        }
        return store;
    }
}

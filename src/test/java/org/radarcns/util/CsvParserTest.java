package org.radarcns.util;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.*;

import java.io.StringReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CsvParserTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void parseLine() throws Exception {
        StringReader input = new StringReader("a,b,c\n0,1,\"1,1\"");
        CsvParser parser = new CsvParser(input);
        assertThat(parser.parseLine(), contains("a", "b", "c"));
        assertThat(parser.parseLine(), contains("0", "1", "1,1"));
    }

    @Test
    public void parseLineException() throws Exception {
        StringReader input = new StringReader("a,b,c\n0,1,1,1");
        CsvParser parser = new CsvParser(input);
        assertThat(parser.parseLine(), contains("a", "b", "c"));
        exception.expect(IllegalArgumentException.class);
        parser.parseLine();
    }
}
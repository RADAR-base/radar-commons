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

import static org.radarcns.integration.model.MockCsvParserUtility.getStartTimeWindow;
import static org.radarcns.integration.model.MockCsvParserUtility.getTimestamp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.radarcns.integration.model.ExpectedValue.ExpectedType;
import org.radarcns.mock.MockDataConfig;
import org.radarcns.util.CsvParser;

/**
 * Starting from a CSV file, this parser generates a map containing all available fields.
 * The {@link Variable#VALUE} field can contain either a Double or an array of Doubles.
 */
public class MockConfigToCsvParser {

    /**
     * Enumerator containing all fields returned by the next function @see {@link
     * MockConfigToCsvParser#next()}.
     **/
    public enum Variable {
        USER("userId"),
        SOURCE("sourceId"),
        TIME_WINDOW("timeWindow"),
        TIMESTAMP("timeReceived"),
        EXPECTED_TYPE("expectedType"),
        VALUE("value");

        private String value;

        private static List<Variable> valiableList = new ArrayList<>();

        Variable(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        @SuppressWarnings("checkstyle:JavadocMethod")
        public String toString() {
            return this.getValue();
        }

        /**
         * @param value representing a {@code Variable} item
         * @return the {@code Variable} that matches the input.
         **/
        public static Variable getEnum(String value) {
            for (Variable v : values()) {
                if (v.getValue().equalsIgnoreCase(value)) {
                    return v;
                }
            }
            throw new IllegalArgumentException();
        }

        /**
         * @return a {@code List} of Variables.
         **/
        public static List<Variable> toList() {
            valiableList.add(USER);
            valiableList.add(SOURCE);
            valiableList.add(TIME_WINDOW);
            valiableList.add(TIMESTAMP);
            valiableList.add(EXPECTED_TYPE);
            valiableList.add(VALUE);
            return valiableList;
        }
    }

    private final CsvParser csvReader;

    private final Map<String, Integer> headerMap;

    private final ExpectedType expecedType;

    private final MockDataConfig config;



    //private static final Logger logger = LoggerFactory.getLogger(MockConfigToCsvParser.class);

    /**
     * Constructor that initialises the {@code CSVReader} and computes the {@code ExpectedType}.
     *
     * @param config containing the CSV file path that has to be parsed
     **/
    public MockConfigToCsvParser(MockDataConfig config)
        throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
        IllegalAccessException, IOException {

        this.config = config;

        Class<?> keyClass = Class.forName(config.getKeySchema());
        Schema keySchema = (Schema) keyClass.getMethod("getClassSchema").invoke(null);
        SpecificData.newInstance(keyClass, keySchema);

        Class<?> valueClass = Class.forName(config.getValueSchema());
        Schema valueSchema = (Schema) valueClass.getMethod("getClassSchema")
                .invoke(null);
        SpecificData.newInstance(valueClass, valueSchema);

        FileReader fr = new FileReader(config.getAbsoluteDataFile());
        csvReader = new CsvParser(new BufferedReader(fr));

        headerMap = new HashMap<>();
        getHeader();

        expecedType = getExpectedType(config);
    }

    /**
     * @return {@code Map} of key {@link Variable} and value Object computed by the next available
     * raw of CSV file.
     **/
    public Map<Variable, Object> next() throws IOException {
        List<String> rawValues = csvReader.parseLine();
        if (rawValues == null) {
            return null;
        }

        HashMap<Variable, Object> map = new HashMap<>();

        for (Variable var : Variable.toList()) {
            map.put(var, computeValue(rawValues, var, config));
        }

        return map;
    }

    /**
     * Close the {@code MockConfigToCsvParser} closing the CSV reader.
     **/
    public void close() throws IOException {
        csvReader.close();
    }

    /**
     * @return {@code ExpectedType} type precomputed during the instantiation.
     **/
    public ExpectedType getExpecedType() {
        return expecedType;
    }

    /**
     * @param config parameters used to compute the returned value.
     * @return the {@code ExpectedType} type based on the input.
     **/
    public static ExpectedType getExpectedType(MockDataConfig config) {
        if (config.getSensor().contains("ACCELEROMETER")) {
            return ExpectedType.ARRAY;
        }

        return ExpectedType.DOUBLE;
    }

    /**
     * @param rawValues array of Strings containing data parsed from the CSV file.
     * @param var variable that has to be extracted from the raw data.
     * @return an {@code Object} representing the required variable.
     **/
    private Object computeValue(List<String> rawValues, Variable var, MockDataConfig config) {
        switch (var) {
            case USER:
                existOrThrow(var.getValue());
                return rawValues.get(headerMap.get(var.getValue()));
            case SOURCE:
                existOrThrow(var.getValue());
                return rawValues.get(headerMap.get(var.getValue()));
            case TIMESTAMP:
                existOrThrow(var.getValue());
                return getTimestamp(rawValues.get(headerMap.get(var.getValue())));
            case VALUE:
                return extractValue(rawValues, config);
            case TIME_WINDOW:
                return getStartTimeWindow(rawValues.get(headerMap.get(
                        Variable.TIMESTAMP.getValue())));
            case EXPECTED_TYPE:
                return expecedType;
            default:
                throw new IllegalArgumentException("Cannot handle variable of type "
                    + var.getValue());
        }
    }

    /**
     * @param rawValues array of Strings containing data parsed from the CSV file.
     * @param config states the variable name that has to be parsed.
     * @return either a {@code Double} or a {@code Double[]} according to the expected value type.
     **/
    //TODO use the MockConfigToCsvParser#parseValue function to verify if
    // the associted Schema is representing the
    // required field has a Double or a List<Double>
    private Object extractValue(List<String> rawValues, MockDataConfig config) {
        if (expecedType.equals(ExpectedType.DOUBLE)) {
            existOrThrow(config.getAssertHeader());

            return Double.parseDouble(rawValues.get(headerMap.get(config.getAssertHeader())));
        } else if (expecedType.equals(ExpectedType.ARRAY)) {

            String[] testCase = config.getAssertHeader().split(", ");
            Double[] value = new Double[testCase.length];

            for (int i = 0; i < testCase.length; i++) {
                existOrThrow(testCase[i]);
                value[i] = Double.parseDouble(rawValues.get(headerMap.get(testCase[i])));
            }

            return value;
        }

        throw new IllegalArgumentException("Illegale expected Type " + expecedType.getValue());
    }

    /**
     * Initialise the {@code HashMap} useful for converting a variable name to the relative index
     * in the raw data array.
     **/
    public void getHeader() throws IOException {
        List<String> header = csvReader.parseLine();

        for (int i = 0; i < header.size(); i++) {
            headerMap.put(header.get(i), i);
        }
    }



    /**
     * @param key field name that has to be verified.
     * @throws IllegalArgumentException if the headers map does not contain the input key.
     **/
    private void existOrThrow(String key) {
        if (!headerMap.containsKey(key)) {
            throw new IllegalArgumentException("Headers does not contain " + key);
        }
    }
}
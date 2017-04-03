package org.radarcns.mock.data;

/*
 *  Copyright 2016 Kings College London and The Hyve
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.radarcns.util.Metronome;
import org.radarcns.util.Strings;


/**
 * Generic CSV sensor definition.
 */
public abstract class CsvSensorDataModel {

    public static final String USER_ID_MOCK = "UserID_0";
    public static final String SOURCE_ID_MOCK = "SourceID_0";

    private List<String> headers;
    private String user;
    private String source;

    //private static final Logger logger = LoggerFactory.getLogger(CsvSensorDataModel.class);

    /**
     * Constructor.
     * @param headers {@code List<String>} containing the fields name that has to be generated
     */
    public CsvSensorDataModel(List<String> headers) {
        this(headers, null, null, null);
    }

    /**
     * Constructor.
     * @param headers {@code List<String>} containing the fields name that has to be generated
     * @param user user identifier
     * @param source source identifier
     * @param timeZero initial instant used to compute all needed instants
     */
    public CsvSensorDataModel(List<String> headers, String user, String source, Long timeZero) {
        this.headers = new ArrayList<>(4 + headers.size());
        this.headers.add("userId");
        this.headers.add("sourceId");
        this.headers.add("time");
        this.headers.add("timeReceived");

        this.headers.addAll(headers);

        this.user = user;
        this.source = source;

        if (Strings.isNullOrEmpty(this.user)) {
            this.user = USER_ID_MOCK;
        }

        if (Strings.isNullOrEmpty(this.source)) {
            this.source = SOURCE_ID_MOCK;
        }
    }

    public String getUser() {
        return user;
    }

    public String getSource() {
        return source;
    }

    /**
     * @return a comma separated {@code String} containing all header names.
     **/
    public List<String> getHeaders() {
        return headers;
    }

    /**
     * Simulates data of a sensor with the given frequency for a time interval specified by
     *      duration.
     * @param duration in seconds for the simulation
     * @param frequency of the simulated sensor. Amount of messages generated in 1 second
     * @return list containing simulated values
     */
    public Iterator<List<String>> iterateValues(final long duration, final int frequency) {
        return new Iterator<List<String>>() {
            final Metronome timestamps = new Metronome(duration * frequency, frequency);

            @Override
            public boolean hasNext() {
                return timestamps.hasNext();
            }

            @Override
            public List<String> next() {
                if (!hasNext()) {
                    throw new IllegalStateException("Iterator done");
                }
                long time = timestamps.next();
                return Arrays.asList(getUser(),
                        getSource(),
                        getTimestamp(getRandomRoundTripTime(time)),
                        getTimestamp(time),
                        nextValue());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns the next value for the simulation.
     */
    protected abstract String nextValue();

    /**
     * Converts a timestamp in a string stating a timestamp expressed in double.
     * @param time value that has to be converted
     * @return a string value representing a timestamp
     */
    public static String getTimestamp(long time) {
        return  Double.toString(time / 1000d);
    }

    /**
     * @return random {@code Double} using {@code ThreadLocalRandom}.
     **/
    public static double getRandomDouble() {
        return ThreadLocalRandom.current().nextDouble();
    }

    /**
     * It returns a random {@code Double} between min and max.
     * @param min range lower bound
     * @param max range upper bound
     * @return random {@code Double} using {@code ThreadLocalRandom}
     **/
    public static double getRandomDouble(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min , max);
    }

    /**
     * It returns a random {@code Float} between min and max.
     * @param min range lower bound
     * @param max range upper bound
     * @return random {@code Float} using {@code ThreadLocalRandom}
     **/
    public static float getRandomFloat(float min, float max) {
        return (float)getRandomDouble(min, max);
    }

    /**
     * It returns the a random Round Trip Time for the given in input time.
     * @param timeReceived time at which the message has been received
     * @return random {@code Double} representig the Round Trip Time for the given timestamp
     *      using {@code ThreadLocalRandom}
     **/
    public long getRandomRoundTripTime(long timeReceived) {
        return timeReceived - (ThreadLocalRandom.current().nextLong(1 , 10));
    }

}

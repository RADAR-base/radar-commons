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

import static java.util.Collections.singletonList;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import org.radarcns.mock.MockDataConfig;

/**
 * It generates a CVS file that can be used to stream data and
 * to compute the expected results.
 */
public class CsvGenerator {

    private CsvGenerator() {}

    //private static final Logger logger = LoggerFactory.getLogger(CSVGenerator.class);

    public static final String AXIS_X = "x";
    public static final String AXIS_Y = "y";
    public static final String AXIS_Z = "z";
    public static final String BATTERY = "batteryLevel";
    public static final String BLOOD_VOLUME_PULSE = "bloodVolumePulse";
    public static final String ELECTRO_DERMAL_ACTIVITY = "electroDermalActivity";
    public static final String INTER_BEAT_INTERVAL = "interBeatInterval";
    public static final String TEMPERATURE = "temperature";

    /**
     * It generates a CSV file simulating an accelerometer sensor.
     *
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void accelerometer(long duration, int frequency, File file)
            throws IOException {
        accelerometer(null, null, null, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating an accelerometer sensor.
     *
     * @param user user identifier
     * @param source source identifier
     * @param timeZero initial instant used to compute all needed instants
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void accelerometer(String user, String source, Long timeZero, long duration,
            int frequency, File file) throws IOException {
        CsvSensorDataModel accelerator = new CsvSensorDataModel(new ArrayList<String>() {
            {
                add(AXIS_X);
                add(AXIS_Y);
                add(AXIS_Z);
            }
        }, user, source, timeZero) {
            @Override
            public String nextValue() {
                return getRandomFloat(0.0f, 2.0f) + "," + getRandomFloat(0.0f, 2.0f)
                        + "," + getRandomFloat(0.0f, 2.0f);
            }
        };

        writeFile(accelerator, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating battery life decay.
     *
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void battery(long duration, int frequency, File file) throws IOException {
        battery(null, null, null, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating battery life decay.
     *
     * @param user user identifier
     * @param source source identifier
     * @param timeZero initial instant used to compute all needed instants
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void battery(String user, String source, Long timeZero, long duration,
            int frequency, File file) throws IOException {
        CsvSensorDataModel battery = new CsvSensorDataModel(singletonList(BATTERY),
                user, source, timeZero) {

            private double batteryDecayFactor = 0.1f * getRandomDouble();
            private double count = 1;

            @Override
            public String nextValue() {
                Double batteryLevel = 1d - (batteryDecayFactor * count % 1);
                count++;
                return String.valueOf(batteryLevel.floatValue());
            }
        };

        writeFile(battery, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating Photoplethysmograph data.
     *
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void bloodVolumePulse(long duration, int frequency, File file)
            throws IOException {
        bloodVolumePulse(null, null, null, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating Photoplethysmograph data.
     *
     * @param user user identifier
     * @param source source identifier
     * @param timeZero initial instant used to compute all needed instants
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void bloodVolumePulse(String user, String source, Long timeZero, long duration,
            int frequency, File file) throws IOException {
        CsvSensorDataModel bloodVolumePulse = new CsvSensorDataModel(
                singletonList(BLOOD_VOLUME_PULSE), user,
                source, timeZero) {
            @Override
            public String nextValue() {
                return getRandomFloat(60.0f, 90.0f).toString();
            }
        };

        writeFile(bloodVolumePulse, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulatin ggalvanic skin response sensor data.
     *
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void electrodermalActivty(long duration, int frequency, File file)
            throws IOException {
        electrodermalActivty(null, null, null, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulatin ggalvanic skin response sensor data.
     *
     * @param user user identifier
     * @param source source identifier
     * @param timeZero initial instant used to compute all needed instants
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void electrodermalActivty(String user, String source, Long timeZero,
            long duration, int frequency, File file) throws IOException {
        CsvSensorDataModel electrodermalActivty = new CsvSensorDataModel(
                singletonList(ELECTRO_DERMAL_ACTIVITY),
                user, source, timeZero) {
            @Override
            public String nextValue() {
                return getRandomFloat(0.01f, 0.05f).toString();
            }
        };

        writeFile(electrodermalActivty, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating inter beat interval data.
     *
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void interBeatInterval(long duration, int frequency, File file)
            throws IOException {
        interBeatInterval(null, null, null, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating inter beat interval data.
     *
     * @param user user identifier
     * @param source source identifier
     * @param timeZero initial instant used to compute all needed instants
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void interBeatInterval(String user, String source, Long timeZero, long duration,
            int frequency, File file) throws IOException {
        CsvSensorDataModel interBeatInterval = new CsvSensorDataModel(
                singletonList(INTER_BEAT_INTERVAL),
                user, source, timeZero) {
            @Override
            public String nextValue() {
                return getRandomFloat(55.0f, 120.0f).toString();
            }
        };

        writeFile(interBeatInterval, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating a thermometer sensor.
     *
     * @param duration time in minutes that the simulation will cover
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void thermometer(long duration, int frequency, File file) throws IOException {
        thermometer(null, null, null, duration, frequency, file);
    }

    /**
     * It generates a CSV file simulating a thermometer sensor.
     *
     * @param user user identifier
     * @param source source identifier
     * @param timeZero initial instant used to compute all needed instants
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void thermometer(String user, String source, Long timeZero, long duration,
            int frequency, File file) throws IOException {
        CsvSensorDataModel temperature = new CsvSensorDataModel(singletonList(TEMPERATURE),
                user, source, timeZero) {
            @Override
            public String nextValue() {
                return getRandomFloat(36.5f, 37.0f).toString();
            }
        };

        writeFile(temperature, duration, frequency, file);
    }

    /**
     * It writes a CSV file.
     *
     * @param generator data sample
     * @param frequency number of sample within a second
     * @param file that has to be written
     **/
    public static void writeFile(CsvSensorDataModel generator, long duration, int frequency,
            File file)
            throws IOException {

        try (FileWriter writer = new FileWriter(file, false)) {
            writer.write(generator.getHeaders());

            for (String sample : generator.getValues(duration, frequency)) {
                writer.write(sample);
            }

            writer.flush();
            writer.close();
        }
    }

    /**
     * Generates new CVS file to simulation a single user with a single device as longs as seconds.
     *
     * @param config properties containing metadata to generate data
     * @param duration simulation duration expressed in seconds
     * @param parentFile of csv file to be generate
     * @throws IOException in case configuration file cannot be retrieved
     */
    public static void generate(MockDataConfig config, Long duration, File parentFile)
            throws IOException {
        File file = config.getDataFile(parentFile);

        switch (config.getSensor()) {
            case "ACCELEROMETER":
                accelerometer(duration, config.getFrequency().intValue(), file);
                break;
            case "BATTERY":
                battery(duration, config.getFrequency().intValue(), file);
                break;
            case "BLOOD_VOLUME_PULSE":
                bloodVolumePulse(duration, config.getFrequency().intValue(), file);
                break;
            case "ELECTRODERMAL_ACTIVITY":
                electrodermalActivty(duration, config.getFrequency().intValue(), file);
                break;
            case "HEART_RATE":
                throw new IllegalArgumentException(config.getSensor() + " is not yet supported");
            case "INTER_BEAT_INTERVAL":
                interBeatInterval(duration, config.getFrequency().intValue(), file);
                break;
            case "THERMOMETER":
                thermometer(duration, config.getFrequency().intValue(), file);
                break;
            default:
                throw new
                        IllegalArgumentException(config.getSensor() + " is not yet supported");
        }
    }


}

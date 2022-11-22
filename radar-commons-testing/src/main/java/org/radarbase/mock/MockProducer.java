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

package org.radarbase.mock;

import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import kotlin.Unit;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.avro.SchemaValidationException;
import org.json.JSONObject;
import org.radarbase.config.ServerConfig;
import org.radarbase.config.YamlConfigLoader;
import org.radarbase.mock.config.AuthConfig;
import org.radarbase.mock.config.BasicMockConfig;
import org.radarbase.mock.config.MockDataConfig;
import org.radarbase.mock.data.MockCsvParser;
import org.radarbase.mock.data.RecordGenerator;
import org.radarbase.producer.BatchedKafkaSender;
import org.radarbase.producer.KafkaSender;
import org.radarbase.producer.rest.ConnectionState;
import org.radarbase.producer.rest.RestClient;
import org.radarbase.producer.rest.RestSender;
import org.radarbase.producer.rest.SchemaRetriever;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.empatica.EmpaticaE4Acceleration;
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.passive.empatica.EmpaticaE4ElectroDermalActivity;
import org.radarcns.passive.empatica.EmpaticaE4InterBeatInterval;
import org.radarcns.passive.empatica.EmpaticaE4Temperature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Mock Producer class that can be used to stream data. It can use MockFileSender and MockDevice
 * for testing purposes, with direct or indirect streaming.
 */
@SuppressWarnings("PMD")
public class MockProducer {

    private static final Logger logger = LoggerFactory.getLogger(MockProducer.class);

    private final List<MockDevice<ObservationKey>> devices;
    private final List<MockFileSender> files;
    private final List<KafkaSender> senders;
    private final SchemaRetriever retriever;

    /**
     * MockProducer with files from current directory. The data root directory will be the current
     * directory.
     * @param mockConfig configuration to mock
     * @throws IOException if the data could not be read or sent
     */
    public MockProducer(BasicMockConfig mockConfig) throws IOException {
        this(mockConfig, null);
    }

    /**
     * Basic constructor.
     * @param mockConfig configuration to mock
     * @param root root directory of where mock files are located
     * @throws IOException if data could not be sent
     */
    public MockProducer(BasicMockConfig mockConfig, Path root) throws IOException {
        int numDevices = mockConfig.getNumberOfDevices();

        RestClient restClient = RestClient.Companion.globalRestClient(builder -> {
            builder.setServer(mockConfig.getSchemaRegistry());
            builder.timeout(10, TimeUnit.SECONDS);
            return Unit.INSTANCE;
        });
        retriever = new SchemaRetriever(restClient);
        List<KafkaSender> tmpSenders = null;

        try {
            devices = new ArrayList<>(numDevices);
            files = new ArrayList<>(numDevices);

            List<MockDataConfig> dataConfigs = mockConfig.getData();
            if (dataConfigs == null) {
                dataConfigs = defaultDataConfig();
            }

            List<RecordGenerator<ObservationKey>> generators;
            List<MockCsvParser> mockFiles;
            generators = createGenerators(dataConfigs);
            mockFiles = createMockFiles(dataConfigs, root);

            tmpSenders = createSenders(mockConfig, numDevices + mockFiles.size(),
                    mockConfig.getAuthConfig());

            if (!generators.isEmpty()) {
                String userId = "UserID_";
                String sourceId = "SourceID_";

                for (int i = 0; i < numDevices; i++) {
                    ObservationKey key = new ObservationKey("test", userId + i, sourceId + i);
                    devices.add(new MockDevice<>(tmpSenders.get(i), key, generators));
                }
            }

            for (int i = 0; i < mockFiles.size(); i++) {
                files.add(new MockFileSender(tmpSenders.get(i + numDevices), mockFiles.get(i)));
            }
        } catch (CsvValidationException ex) {
            if (tmpSenders != null) {
                for (KafkaSender sender : tmpSenders) {
                    sender.close();
                }
            }
            throw new IOException("Cannot read CSV file", ex);
        } catch (Exception ex) {
            if (tmpSenders != null) {
                for (KafkaSender sender : tmpSenders) {
                    sender.close();
                }
            }
            throw ex;
        }

        senders = tmpSenders;
    }

    private List<KafkaSender> createSenders(
            BasicMockConfig mockConfig, int numDevices, AuthConfig authConfig) throws IOException {

        return createRestSenders(numDevices, retriever, mockConfig.getRestProxy(),
                mockConfig.hasCompression(), authConfig);
    }

    private String requestAccessToken(OkHttpClient okHttpClient, AuthConfig authConfig)
            throws IOException {
        Request request = new Builder()
                .url(authConfig.getTokenUrl())
                .post(new FormBody.Builder()
                        .add("grant_type", "client_credentials")
                        .add("client_id", authConfig.getClientId())
                        .add("client_secret", authConfig.getClientSecret())
                        .build())
                .addHeader("Authorization", Credentials
                        .basic(authConfig.getClientId(), authConfig.getClientSecret()))
                .build();

        try (Response response = okHttpClient.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            if (responseBody == null) {
                throw new IOException("Cannot request token at " + request.url()
                        + " (" + response.code() + ") returned no body");
            }
            if (!response.isSuccessful()) {
                throw new IOException("Cannot request token: at " + request.url()
                        + " (" + response.code() + "): " + responseBody.string());
            }
            return new JSONObject(responseBody.string()).getString("access_token");
        }
    }

    /** Create senders that produce data to Kafka via the REST proxy. */
    private List<KafkaSender> createRestSenders(int numDevices,
            SchemaRetriever retriever, ServerConfig restProxy, boolean useCompression,
            AuthConfig authConfig) throws IOException {
        List<KafkaSender> result = new ArrayList<>(numDevices);
        ConnectionState sharedState = new ConnectionState(10, TimeUnit.SECONDS);

        Headers headers;
        if (authConfig == null) {
            headers = Headers.of();
        } else {
            OkHttpClient okHttpClient = new OkHttpClient();
            String token = requestAccessToken(okHttpClient, authConfig);
            headers = Headers.of("Authorization", "Bearer " + token);
        }

        for (int i = 0; i < numDevices; i++) {
            RestClient httpClient = RestClient.Companion.newRestClient(builder -> {
                    builder.setServer(restProxy);
                    builder.gzipCompression(useCompression);
                    builder.timeout(10, TimeUnit.SECONDS);
                    return Unit.INSTANCE;
            });

            RestSender restSender = RestSender.Companion.restSender(builder -> {
                    builder.setSchemaRetriever(retriever);
                    builder.setHttpClient(httpClient);
                    builder.setConnectionState(sharedState);
                    builder.setHeaders(headers.newBuilder());
                    return Unit.INSTANCE;
            });
            result.add(new BatchedKafkaSender(restSender, 1000, 1000));
        }
        return result;
    }

    /** Start sending data. */
    public void start() throws IOException {
        for (MockDevice<?> device : devices) {
            device.start();
        }
        for (MockFileSender file : files) {
            file.send();
            logger.info("Sent data {}", file);
        }
    }

    /** Stop sending data and clean up all resources. */
    public void shutdown() throws IOException, InterruptedException, SchemaValidationException {
        if (!devices.isEmpty()) {
            logger.info("Shutting down mock devices");
            for (MockDevice<?> device : devices) {
                device.shutdown();
            }
            logger.info("Waiting for mock devices to finish...");
            for (MockDevice<?> device : devices) {
                device.join(5_000L);
            }
        }
        logger.info("Closing channels");
        for (KafkaSender sender : senders) {
            sender.close();
        }

        for (MockDevice<?> device : devices) {
            device.checkException();
        }
    }

    /**
     * Runs the MockProducer with given YAML mock config file.
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            logger.error("This command needs a mock file argument");
            System.exit(1);
        }

        Path mockFile = Paths.get(args[0]).toAbsolutePath();
        BasicMockConfig config = null;
        try {
            config = new YamlConfigLoader().load(mockFile, BasicMockConfig.class);
        } catch (IOException ex) {
            logger.error("Failed to load given mock file {}: {}", mockFile, ex.getMessage());
            System.exit(1);
        }

        try {
            MockProducer producer = new MockProducer(config, mockFile.getParent());
            producer.start();
            if (!producer.devices.isEmpty()) {
                waitForProducer(producer, config.getDuration());
            }
        } catch (IllegalArgumentException ex) {
            logger.error("{}", ex.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            // during shutdown, not that important. Will shutdown again.
        } catch (Exception ex) {
            logger.error("Failed to start mock producer", ex);
            System.exit(1);
        }
    }

    /** Wait for given duration and then stop the producer. */
    private static void waitForProducer(final MockProducer producer, long duration)
            throws IOException, InterruptedException, SchemaValidationException {
        final AtomicBoolean isShutdown = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (!isShutdown.get()) {
                    producer.shutdown();
                }
            } catch (InterruptedException ex) {
                logger.warn("Shutdown interrupted", ex);
            } catch (Exception ex) {
                logger.warn("Failed to shutdown producer", ex);
            }
        }));

        if (duration <= 0L) {
            try {
                logger.info("Producing data until interrupted");
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException ex) {
                // this is intended
            }
        } else {
            try {
                logger.info("Producing data for {} seconds", duration / 1000d);
                Thread.sleep(duration);
            } catch (InterruptedException ex) {
                logger.warn("Data producing interrupted");
            }
            producer.shutdown();
            isShutdown.set(true);
            logger.info("Producing data done.");
        }
    }

    private List<MockDataConfig> defaultDataConfig() {
        MockDataConfig acceleration = new MockDataConfig();
        acceleration.setTopic("android_empatica_e4_acceleration");
        acceleration.setFrequency(32);
        acceleration.setValueSchema(EmpaticaE4Acceleration.class.getName());
        acceleration.setInterval(-2d, 2d);
        acceleration.setValueFields(Arrays.asList("x", "y", "z"));

        MockDataConfig battery = new MockDataConfig();
        battery.setTopic("android_empatica_e4_battery_level");
        battery.setValueSchema(EmpaticaE4BatteryLevel.class.getName());
        battery.setFrequency(1);
        battery.setInterval(0d, 1d);
        battery.setValueField("batteryLevel");

        MockDataConfig bvp = new MockDataConfig();
        bvp.setTopic("android_empatica_e4_blood_volume_pulse");
        bvp.setValueSchema(EmpaticaE4BloodVolumePulse.class.getName());
        bvp.setFrequency(64);
        bvp.setInterval(60d, 90d);
        bvp.setValueField("bloodVolumePulse");

        MockDataConfig eda = new MockDataConfig();
        eda.setTopic("android_empatica_e4_electrodermal_activity");
        eda.setValueSchema(EmpaticaE4ElectroDermalActivity.class.getName());
        eda.setValueField("electroDermalActivity");
        eda.setFrequency(4);
        eda.setInterval(0.01d, 0.05d);

        MockDataConfig ibi = new MockDataConfig();
        ibi.setTopic("android_empatica_e4_inter_beat_interval");
        ibi.setValueSchema(EmpaticaE4InterBeatInterval.class.getName());
        ibi.setValueField("interBeatInterval");
        ibi.setFrequency(1);
        ibi.setInterval(40d, 150d);

        MockDataConfig temperature = new MockDataConfig();
        temperature.setTopic("android_empatica_e4_temperature");
        temperature.setValueSchema(EmpaticaE4Temperature.class.getName());
        temperature.setFrequency(4);
        temperature.setInterval(20d, 60d);
        temperature.setValueField("temperature");

        return Arrays.asList(acceleration, battery, bvp, eda, ibi, temperature);
    }

    private List<RecordGenerator<ObservationKey>> createGenerators(List<MockDataConfig> configs) {

        List<RecordGenerator<ObservationKey>> result = new ArrayList<>(configs.size());

        for (MockDataConfig config : configs) {
            if (config.getDataFile() == null) {
                result.add(new RecordGenerator<>(config, ObservationKey.class));
            }
        }

        return result;
    }

    private List<MockCsvParser> createMockFiles(List<MockDataConfig> configs,
            Path dataRoot) throws IOException, CsvValidationException {

        List<MockCsvParser> result = new ArrayList<>(configs.size());

        Instant now = Instant.now();
        Path parent = dataRoot;
        if (parent == null) {
            parent = Paths.get(".").toAbsolutePath();
        }

        for (MockDataConfig config : configs) {
            if (config.getDataFile() != null) {
                logger.info("Reading mock data from {}", config.getDataFile());
                result.add(new MockCsvParser(config, parent, now, retriever));
            } else {
                logger.info("Generating mock data from {}", config);
            }
        }

        return result;
    }
}

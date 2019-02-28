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

package org.radarcns.mock;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.SchemaValidationException;
import org.radarcns.config.ServerConfig;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.mock.config.BasicMockConfig;
import org.radarcns.mock.config.MockDataConfig;
import org.radarcns.mock.data.MockCsvParser;
import org.radarcns.mock.data.RecordGenerator;
import org.radarcns.passive.empatica.EmpaticaE4Acceleration;
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.passive.empatica.EmpaticaE4ElectroDermalActivity;
import org.radarcns.passive.empatica.EmpaticaE4InterBeatInterval;
import org.radarcns.passive.empatica.EmpaticaE4Temperature;
import org.radarcns.producer.BatchedKafkaSender;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.direct.DirectSender;
import org.radarcns.producer.rest.ConnectionState;
import org.radarcns.producer.rest.RestClient;
import org.radarcns.producer.rest.RestSender;
import org.radarcns.producer.rest.SchemaRetriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Mock Producer class that can be used to stream data. It can use MockFileSender and MockDevice
 * for testing purposes, with direct or indirect streaming.
 */
@SuppressWarnings("PMD.DoNotCallSystemExit")
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

        retriever = new SchemaRetriever(mockConfig.getSchemaRegistry(), 10);
        List<KafkaSender> tmpSenders = null;

        try {
            devices = new ArrayList<>(numDevices);
            files = new ArrayList<>(numDevices);

            List<MockDataConfig> dataConfigs = mockConfig.getData();
            if (dataConfigs == null) {
                dataConfigs = defaultDataConfig();
            }

            List<RecordGenerator<ObservationKey>> generators;
            List<MockCsvParser<ObservationKey>> mockFiles;
            generators = createGenerators(dataConfigs);
            mockFiles = createMockFiles(dataConfigs, root);

            tmpSenders = createSenders(mockConfig, numDevices + mockFiles.size());

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
            BasicMockConfig mockConfig, int numDevices) {

        if (mockConfig.isDirectProducer()) {
            return createDirectSenders(numDevices, mockConfig.getSchemaRegistry().getUrlString(),
                    mockConfig.getBrokerPaths());
        } else {
            return createRestSenders(numDevices, retriever, mockConfig.getRestProxy(),
                    mockConfig.hasCompression());
        }
    }

    /** Create senders that directly produce data to Kafka. */
    private List<KafkaSender> createDirectSenders(int numDevices,
            String retrieverUrl, String brokerPaths) {
        List<KafkaSender> result = new ArrayList<>(numDevices);
        for (int i = 0; i < numDevices; i++) {
            Properties properties = new Properties();
            properties.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            properties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            properties.put(SCHEMA_REGISTRY_URL_CONFIG, retrieverUrl);
            properties.put(BOOTSTRAP_SERVERS_CONFIG, brokerPaths);

            result.add(new DirectSender(properties));
        }
        return result;
    }

    /** Create senders that produce data to Kafka via the REST proxy. */
    private List<KafkaSender> createRestSenders(int numDevices,
            SchemaRetriever retriever, ServerConfig restProxy, boolean useCompression) {
        List<KafkaSender> result = new ArrayList<>(numDevices);
        ConnectionState sharedState = new ConnectionState(10, TimeUnit.SECONDS);

        for (int i = 0; i < numDevices; i++) {
            RestClient httpClient = RestClient.newClient()
                    .server(restProxy)
                    .gzipCompression(useCompression)
                    .timeout(10, TimeUnit.SECONDS)
                    .build();

            RestSender restSender = new RestSender.Builder()
                    .schemaRetriever(retriever)
                    .httpClient(httpClient)
                    .connectionState(sharedState)
                    .build();
            result.add(new BatchedKafkaSender(restSender, 1000, 1000));
        }
        return result;
    }

    /** Start sending data. */
    public void start() throws IOException {
        for (MockDevice device : devices) {
            device.start();
        }
        for (MockFileSender file : files) {
            file.send();
        }
    }

    /** Stop sending data and clean up all resources. */
    public void shutdown() throws IOException, InterruptedException, SchemaValidationException {
        if (!devices.isEmpty()) {
            logger.info("Shutting down mock devices");
            for (MockDevice device : devices) {
                device.shutdown();
            }
            logger.info("Waiting for mock devices to finish...");
            for (MockDevice device : devices) {
                device.join(5_000L);
            }
        }
        logger.info("Closing channels");
        for (KafkaSender sender : senders) {
            sender.close();
        }

        for (MockDevice device : devices) {
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
            waitForProducer(producer, config.getDuration());
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

    private List<MockCsvParser<ObservationKey>> createMockFiles(List<MockDataConfig> configs,
            Path dataRoot) throws IOException {

        List<MockCsvParser<ObservationKey>> result = new ArrayList<>(configs.size());

        Path parent = dataRoot;
        if (parent == null) {
            parent = Paths.get(".").toAbsolutePath();
        }

        for (MockDataConfig config : configs) {
            if (config.getDataFile() != null) {
                result.add(new MockCsvParser<>(config, parent));
            }
        }

        return result;
    }
}

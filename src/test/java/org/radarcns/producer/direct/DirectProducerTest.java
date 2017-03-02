package org.radarcns.producer.direct;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import junit.framework.TestCase;

import org.radarcns.key.MeasurementKey;
import org.radarcns.mock.MockDevice;
import org.radarcns.producer.KafkaSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class DirectProducerTest extends TestCase {

    private static final Logger logger = LoggerFactory.getLogger(DirectProducerTest.class);

    public void testDirect() throws InterruptedException, IOException {

        Properties props = new Properties();
        props.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props
                .put(SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");

        if (!Boolean.parseBoolean(props.getProperty("servertest","false"))) {
            logger.info("Serve test case has been disable.");
            return;
        }

        int numberOfDevices = 1;
        logger.info("Simulating the load of " + numberOfDevices);

        String userID = "UserID_";
        String sourceID = "SourceID_";

        MockDevice[] threads = new MockDevice[numberOfDevices];
        KafkaSender[] senders = new KafkaSender[numberOfDevices];
        for (int i = 0; i < numberOfDevices; i++) {
            senders[i] = new DirectSender(props);
            //noinspection unchecked
            threads[i] = new MockDevice<>(senders[i], new MeasurementKey(userID+i, sourceID+i), MeasurementKey.getClassSchema(), MeasurementKey.class);
            threads[i].start();
        }
        long streamingTimeoutMs = 5_000L;
        if (props.containsKey("streaming.timeout.ms")) {
            try {
                streamingTimeoutMs = Long.parseLong(props.getProperty("streaming.timeout.ms"));
            } catch (NumberFormatException ex) {
                // whatever
            }
        }
        threads[0].join(streamingTimeoutMs);
        for (MockDevice device : threads) {
            device.interrupt();
        }
        for (MockDevice device : threads) {
            device.join();
        }
        for (KafkaSender sender : senders) {
            try {
                sender.close();
            } catch (IOException e) {
                logger.warn("Failed to close sender", e);
            }
        }
    }
}

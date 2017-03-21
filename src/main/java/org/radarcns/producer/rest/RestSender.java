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

package org.radarcns.producer.rest;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import okio.Okio;
import okio.Sink;
import org.apache.avro.Schema;
import org.radarcns.config.ServerConfig;
import org.radarcns.data.AvroEncoder;
import org.radarcns.data.Record;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.producer.SchemaRetriever;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RestSender sends records to the Kafka REST Proxy. It does so using an Avro JSON encoding. A new
 * sender must be constructed with {@link #sender(AvroTopic)} per AvroTopic. This implementation is
 * blocking and unbuffered, so flush, clear and close do not do anything. To get a non-blocking
 * sender, wrap this in a {@link ThreadedKafkaSender}, for a buffered sender, wrap it in a
 * {@link BatchedKafkaSender}.
 *
 * @param <K> base key class
 * @param <V> base value class
 */
public class RestSender<K, V> implements KafkaSender<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(RestSender.class);
    private final AvroEncoder keyEncoder;
    private final AvroEncoder valueEncoder;
    private final JsonFactory jsonFactory;
    public static final String KAFKA_REST_ACCEPT_ENCODING =
            "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json";
    public static final MediaType KAFKA_REST_AVRO_ENCODING =
            MediaType.parse("application/vnd.kafka.avro.v1+json; charset=utf-8");

    private HttpUrl schemalessKeyUrl;
    private HttpUrl schemalessValueUrl;
    private Request isConnectedRequest;
    private SchemaRetriever schemaRetriever;
    private RestClient httpClient;

    /**
     * Construct a RestSender.
     * @param kafkaConfig non-null server to send data to
     * @param schemaRetriever non-null Retriever of avro schemas
     * @param keyEncoder non-null Avro encoder for keys
     * @param valueEncoder non-null Avro encoder for values
     * @param connectionTimeout socket connection timeout in seconds
     */
    public RestSender(ServerConfig kafkaConfig, SchemaRetriever schemaRetriever,
            AvroEncoder keyEncoder, AvroEncoder valueEncoder,
            long connectionTimeout) {
        Objects.requireNonNull(kafkaConfig);
        Objects.requireNonNull(schemaRetriever);
        Objects.requireNonNull(keyEncoder);
        Objects.requireNonNull(valueEncoder);
        this.schemaRetriever = schemaRetriever;
        this.keyEncoder = keyEncoder;
        this.valueEncoder = valueEncoder;
        this.jsonFactory = new JsonFactory();
        setRestClient(new RestClient(kafkaConfig, connectionTimeout));
    }

    public synchronized void setConnectionTimeout(long connectionTimeout) {
        if (connectionTimeout != httpClient.getTimeout()) {
            httpClient = new RestClient(httpClient.getConfig(), connectionTimeout);
        }
    }

    public synchronized void setKafkaConfig(ServerConfig kafkaConfig) {
        Objects.requireNonNull(kafkaConfig);
        if (kafkaConfig.equals(httpClient.getConfig())) {
            return;
        }
        setRestClient(new RestClient(kafkaConfig, httpClient.getTimeout()));
    }

    private void setRestClient(RestClient newClient) {
        try {
            schemalessKeyUrl = HttpUrl.get(newClient.getRelativeUrl("topics/schemaless-key"));
            schemalessValueUrl = HttpUrl.get(newClient.getRelativeUrl("topics/schemaless-value"));
            isConnectedRequest = newClient.requestBuilder("").head().build();
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Schemaless topics do not have a valid URL", ex);
        }
        httpClient = newClient;
    }

    public final synchronized void setSchemaRetriever(SchemaRetriever retriever) {
        this.schemaRetriever = retriever;
    }

    private synchronized RestClient getRestClient() {
        return httpClient;
    }

    private synchronized SchemaRetriever getSchemaRetriever() {
        return this.schemaRetriever;
    }

    private synchronized HttpUrl getSchemalessValueUrl() {
        return schemalessValueUrl;
    }

    private synchronized HttpUrl getSchemalessKeyUrl() {
        return schemalessKeyUrl;
    }

    private synchronized Request getIsConnectedRequest() {
        return isConnectedRequest;
    }

    private class RestTopicSender<L extends K, W extends V> implements KafkaTopicSender<L, W> {
        private long lastOffsetSent = -1L;
        private final AvroTopic<L, W> topic;
        private final HttpUrl url;
        private final TopicRequestBody requestBody;

        private RestTopicSender(AvroTopic<L, W> topic) throws IOException {
            this.topic = topic;
            URL rawUrl = getRestClient().getRelativeUrl("topics/" + topic.getName());
            url = HttpUrl.get(rawUrl);
            if (url == null) {
                throw new MalformedURLException("Cannot parse " + rawUrl);
            }
            requestBody = new TopicRequestBody();
        }

        @Override
        public void send(long offset, L key, W value) throws IOException {
            List<Record<L, W>> records = new ArrayList<>(1);
            records.add(new Record<>(offset, key, value));
            send(records);
        }

        /**
         * Actually make a REST request to the Kafka REST server and Schema Registry.
         *
         * @param records values to send
         * @throws IOException if records could not be sent
         */
        @Override
        public void send(List<Record<L, W>> records) throws IOException {
            if (records.isEmpty()) {
                return;
            }
            // Get schema IDs
            Schema valueSchema = topic.getValueSchema();
            String sendTopic = topic.getName();

            HttpUrl sendToUrl = url;

            requestBody.reset();
            try {
                ParsedSchemaMetadata metadata = getSchemaRetriever()
                        .getOrSetSchemaMetadata(sendTopic, false, topic.getKeySchema(), -1);
                requestBody.keySchemaId = metadata.getId();
            } catch (IOException ex) {
                sendToUrl = getSchemalessKeyUrl();
            }
            if (requestBody.keySchemaId == null) {
                requestBody.keySchemaString = topic.getKeySchema().toString();
            }

            try {
                ParsedSchemaMetadata metadata = getSchemaRetriever().getOrSetSchemaMetadata(
                        sendTopic, true, valueSchema, -1);
                requestBody.valueSchemaId = metadata.getId();
            } catch (IOException ex) {
                sendToUrl = getSchemalessValueUrl();
            }
            if (requestBody.valueSchemaId == null) {
                requestBody.valueSchemaString = topic.getValueSchema().toString();
            }
            requestBody.records = records;

            Request request = new Request.Builder()
                    .url(sendToUrl)
                    .addHeader("Accept", KAFKA_REST_ACCEPT_ENCODING)
                    .post(requestBody)
                    .build();

            try (Response response = getRestClient().request(request)) {
                // Evaluate the result
                if (response.isSuccessful()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Added message to topic {} -> {}",
                                sendTopic, response.body().string());
                    }
                    lastOffsetSent = records.get(records.size() - 1).offset;
                } else {
                    String content = response.body().string();
                    logger.error("FAILED to transmit message: {} -> {}...",
                            content, requestBody.content().substring(0, 255));
                    throw new IOException("Failed to submit (HTTP status code " + response.code()
                            + "): " + content);
                }
            } catch (IOException ex) {
                logger.error("FAILED to transmit message:\n{}...",
                        requestBody.content().substring(0, 255) );
                throw ex;
            }
        }

        @Override
        public long getLastSentOffset() {
            return lastOffsetSent;
        }


        @Override
        public void clear() {
            // noop
        }

        @Override
        public void flush() {
            // noop
        }

        @Override
        public void close() {
            // noop
        }

        private class TopicRequestBody extends RequestBody {
            private Integer keySchemaId;
            private Integer valueSchemaId;
            private String keySchemaString;
            private String valueSchemaString;
            private final AvroEncoder.AvroWriter<L> keyWriter;
            private final AvroEncoder.AvroWriter<W> valueWriter;

            private List<Record<L, W>> records;

            private TopicRequestBody() throws IOException {
                keyWriter = keyEncoder.writer(topic.getKeySchema(), topic.getKeyClass());
                valueWriter = valueEncoder.writer(topic.getValueSchema(), topic.getValueClass());
            }

            @Override
            public MediaType contentType() {
                return KAFKA_REST_AVRO_ENCODING;
            }

            @Override
            public void writeTo(BufferedSink sink) throws IOException {
                try (OutputStream out = sink.outputStream();
                     JsonGenerator writer = jsonFactory.createGenerator(out, JsonEncoding.UTF8)) {
                    writer.writeStartObject();
                    if (keySchemaId != null) {
                        writer.writeNumberField("key_schema_id", keySchemaId);
                    } else {
                        writer.writeStringField("key_schema", keySchemaString);
                    }

                    if (valueSchemaId != null) {
                        writer.writeNumberField("value_schema_id", valueSchemaId);
                    } else {
                        writer.writeStringField("value_schema", valueSchemaString);
                    }

                    writer.writeArrayFieldStart("records");

                    for (Record<L, W> record : records) {
                        writer.writeStartObject();
                        writer.writeFieldName("key");
                        writer.writeRawValue(new String(keyWriter.encode(record.key)));
                        writer.writeFieldName("value");
                        writer.writeRawValue(new String(valueWriter.encode(record.value)));
                        writer.writeEndObject();
                    }
                    writer.writeEndArray();
                    writer.writeEndObject();
                }
            }

            private String content() throws IOException {
                try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                        Sink sink = Okio.sink(out);
                        BufferedSink bufferedSink = Okio.buffer(sink)) {
                    writeTo(bufferedSink);
                    return out.toString();
                }
            }

            private void reset() {
                keySchemaId = null;
                keySchemaString = null;
                valueSchemaId = null;
                valueSchemaString = null;
                records = null;
            }
        }
    }

    @Override
    public <L extends K, W extends V> KafkaTopicSender<L, W> sender(AvroTopic<L, W> topic)
            throws IOException {
        return new RestTopicSender<>(topic);
    }

    @Override
    public boolean resetConnection() {
        return isConnected();
    }

    public boolean isConnected() {
        try (Response response = httpClient.request(getIsConnectedRequest())) {
            if (response.isSuccessful()) {
                return true;
            } else {
                logger.warn("Failed to make heartbeat request to {} (HTTP status code {}): {}",
                        httpClient, response.code(), response.body().string());
                return false;
            }
        } catch (IOException ex) {
            // no stack trace is needed
            logger.warn("Failed to make heartbeat request to {}: {}", httpClient, ex.toString());
            return false;
        }
    }

    @Override
    public void close() {
        // noop
    }
}

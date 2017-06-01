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

package org.radarcns.producer.rest;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.avro.Schema;
import org.radarcns.config.ServerConfig;
import org.radarcns.data.AvroEncoder;
import org.radarcns.data.Record;
import org.radarcns.producer.AuthenticationException;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.producer.SchemaRetriever;
import org.radarcns.producer.rest.ConnectionState.State;
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
@SuppressWarnings("PMD.GodClass")
public class RestSender<K, V> implements KafkaSender<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(RestSender.class);
    public static final String KAFKA_REST_ACCEPT_ENCODING =
            "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json";
    public static final String KAFKA_REST_ACCEPT_LEGACY_ENCODING =
            "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json";
    public static final MediaType KAFKA_REST_AVRO_ENCODING =
            MediaType.parse("application/vnd.kafka.avro.v2+json; charset=utf-8");
    public static final MediaType KAFKA_REST_AVRO_LEGACY_ENCODING =
            MediaType.parse("application/vnd.kafka.avro.v1+json; charset=utf-8");

    private final AvroEncoder keyEncoder;
    private final AvroEncoder valueEncoder;
    private final JsonFactory jsonFactory;

    private HttpUrl schemalessKeyUrl;
    private HttpUrl schemalessValueUrl;
    private Request isConnectedRequest;
    private SchemaRetriever schemaRetriever;
    private RestClient httpClient;
    private String acceptType;
    private MediaType contentType;
    private boolean useCompression;
    private final ConnectionState state;
    private String token;

    /**
     * Construct a RestSender.
     * @param httpClient client to send requests with
     * @param schemaRetriever non-null Retriever of avro schemas
     * @param keyEncoder non-null Avro encoder for keys
     * @param valueEncoder non-null Avro encoder for values
     * @param useCompression use compression to send data
     * @param sharedState shared connection state
     * @param token access accessToken for the service; if null, no access accessToken is used
     */
    private RestSender(RestClient httpClient, SchemaRetriever schemaRetriever,
            AvroEncoder keyEncoder, AvroEncoder valueEncoder, boolean useCompression,
            ConnectionState sharedState, String token) {
        this.schemaRetriever = schemaRetriever;
        this.keyEncoder = keyEncoder;
        this.valueEncoder = valueEncoder;
        this.token = token;
        this.jsonFactory = new JsonFactory();
        this.useCompression = useCompression;
        this.acceptType = KAFKA_REST_ACCEPT_ENCODING;
        this.contentType = KAFKA_REST_AVRO_ENCODING;
        this.state = sharedState;
        setRestClient(httpClient);
    }

    public synchronized void setConnectionTimeout(long connectionTimeout) {
        if (connectionTimeout != httpClient.getTimeout()) {
            RestClient newRestClient = new RestClient(httpClient.getConfig(),
                    connectionTimeout, httpClient.getConnectionPool());
            httpClient.close();
            httpClient = newRestClient;
            state.setTimeout(connectionTimeout, TimeUnit.SECONDS);
        }
    }

    public synchronized void setKafkaConfig(ServerConfig kafkaConfig) {
        Objects.requireNonNull(kafkaConfig);
        if (kafkaConfig.equals(httpClient.getConfig())) {
            return;
        }
        RestClient newRestClient = new RestClient(kafkaConfig, httpClient.getTimeout(),
                httpClient.getConnectionPool());
        httpClient.close();
        setRestClient(newRestClient);
    }

    public synchronized void setAccessToken(String token) {
        this.token = token;
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

    public synchronized void setCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    private synchronized boolean hasCompression() {
        return this.useCompression;
    }

    private class RestTopicSender<L extends K, W extends V> implements KafkaTopicSender<L, W> {
        private long lastOffsetSent = -1L;
        private final AvroTopic<L, W> topic;
        private final HttpUrl url;
        private final TopicRequestData<L, W> requestData;

        private RestTopicSender(AvroTopic<L, W> topic) throws IOException {
            this.topic = topic;
            URL rawUrl = getRestClient().getRelativeUrl("topics/" + topic.getName());
            url = HttpUrl.get(rawUrl);
            if (url == null) {
                throw new MalformedURLException("Cannot parse " + rawUrl);
            }
            requestData = new TopicRequestData<>(topic, keyEncoder, valueEncoder, jsonFactory);
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

            Request request = buildRequest(records);

            boolean doResend = false;
            try (Response response = getRestClient().request(request)) {
                // Evaluate the result
                if (response.isSuccessful()) {
                    state.didConnect();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Added message to topic {} -> {}",
                                topic, response.body().string());
                    }
                    lastOffsetSent = records.get(records.size() - 1).offset;
                } else if (response.code() == 401) {
                    throw new AuthenticationException("Cannot authenticate");
                } else if (response.code() == 415
                        && request.header("Accept").equals(KAFKA_REST_ACCEPT_ENCODING)) {
                    state.didConnect();
                    logger.warn("Latest Avro encoding is not supported. Switching to legacy "
                            + "encoding.");
                    synchronized (RestSender.this) {
                        contentType = KAFKA_REST_AVRO_LEGACY_ENCODING;
                        acceptType = KAFKA_REST_ACCEPT_LEGACY_ENCODING;
                    }
                    doResend = true;
                } else {
                    state.didDisconnect();
                    String content = response.body().string();
                    String requestContent = ((TopicRequestBody)request.body()).content();
                    requestContent = requestContent.substring(0,
                            Math.min(requestContent.length(), 255));
                    logger.error("FAILED to transmit message: {} -> {}...",
                            content, requestContent);
                    throw new IOException("Failed to submit (HTTP status code " + response.code()
                            + "): " + content);
                }
            } catch (IOException ex) {
                state.didDisconnect();
                String requestContent = ((TopicRequestBody)request.body()).content();
                requestContent = requestContent.substring(0,
                        Math.min(requestContent.length(), 255));
                logger.error("FAILED to transmit message:\n{}...", requestContent);
                throw ex;
            } finally {
                requestData.reset();
            }

            if (doResend) {
                send(records);
            }
        }

        private Request buildRequest(List<Record<L, W>> records) throws IOException {
            HttpUrl sendToUrl = updateRequestData(records);

            MediaType currentContentType;
            String currentAcceptType;
            String currentToken;

            synchronized (RestSender.this) {
                currentContentType = contentType;
                currentAcceptType = acceptType;
                currentToken = token;
            }

            TopicRequestBody requestBody;
            Request.Builder requestBuilder = new Request.Builder()
                    .url(sendToUrl)
                    .addHeader("Accept", currentAcceptType);

            if (currentToken != null) {
                requestBuilder.addHeader("Authorization", "Bearer " + token);
            }

            if (hasCompression()) {
                requestBody = new GzipTopicRequestBody(requestData, currentContentType);
                requestBuilder.addHeader("Content-Encoding", "gzip");
            } else {
                requestBody = new TopicRequestBody(requestData, currentContentType);
            }

            return requestBuilder.post(requestBody).build();
        }

        private HttpUrl updateRequestData(List<Record<L, W>> records) {
            // Get schema IDs
            Schema valueSchema = topic.getValueSchema();
            String sendTopic = topic.getName();

            HttpUrl sendToUrl = url;

            try {
                ParsedSchemaMetadata metadata = getSchemaRetriever()
                        .getOrSetSchemaMetadata(sendTopic, false, topic.getKeySchema(), -1);
                requestData.setKeySchemaId(metadata.getId());
            } catch (IOException ex) {
                logger.error("Failed to get schema for key {} of topic {}",
                        topic.getKeyClass().getName(), topic, ex);
                sendToUrl = getSchemalessKeyUrl();
            }
            if (requestData.getKeySchemaId() == null) {
                requestData.setKeySchemaString(topic.getKeySchema().toString());
            }

            try {
                ParsedSchemaMetadata metadata = getSchemaRetriever().getOrSetSchemaMetadata(
                        sendTopic, true, valueSchema, -1);
                requestData.setValueSchemaId(metadata.getId());
            } catch (IOException ex) {
                logger.error("Failed to get schema for value {} of topic {}",
                        topic.getValueClass().getName(), topic, ex);
                sendToUrl = getSchemalessValueUrl();
            }
            if (requestData.getValueSchemaId() == null) {
                requestData.setValueSchemaString(topic.getValueSchema().toString());
            }
            requestData.setRecords(records);

            return sendToUrl;
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

    }

    @Override
    public <L extends K, W extends V> KafkaTopicSender<L, W> sender(AvroTopic<L, W> topic)
            throws IOException {
        return new RestTopicSender<>(topic);
    }

    @Override
    public boolean resetConnection() {
        if (state.getState() == State.CONNECTED) {
            return true;
        }
        try (Response response = httpClient.request(getIsConnectedRequest())) {
            if (response.isSuccessful()) {
                state.didConnect();
                return true;
            } else {
                state.didDisconnect();
                logger.warn("Failed to make heartbeat request to {} (HTTP status code {}): {}",
                        httpClient, response.code(), response.body().string());
                return false;
            }
        } catch (IOException ex) {
            // no stack trace is needed
            state.didDisconnect();
            logger.warn("Failed to make heartbeat request to {}: {}", httpClient, ex.toString());
            return false;
        }
    }

    public boolean isConnected() {
        switch (state.getState()) {
            case CONNECTED:
                return true;
            case DISCONNECTED:
                return false;
            case UNKNOWN:
                return resetConnection();
            default:
                throw new IllegalStateException("Illegal connection state");
        }
    }

    @Override
    public void close() {
        httpClient.close();
    }

    public static class Builder<K, V> {
        private ServerConfig kafkaConfig;
        private SchemaRetriever retriever;
        private AvroEncoder keyEncoder;
        private AvroEncoder valueEncoder;
        private boolean compression = false;
        private long timeout = 10;
        private ConnectionState state;
        private ManagedConnectionPool pool;
        private String token;

        public Builder<K, V> server(ServerConfig kafkaConfig) {
            this.kafkaConfig = kafkaConfig;
            return this;
        }

        public Builder<K, V> schemaRetriever(SchemaRetriever schemaRetriever) {
            this.retriever = schemaRetriever;
            return this;
        }

        public Builder<K, V> encoders(AvroEncoder keyEncoder, AvroEncoder valueEncoder) {
            this.keyEncoder = keyEncoder;
            this.valueEncoder = valueEncoder;
            return this;
        }

        public Builder<K, V> useCompression(boolean compression) {
            this.compression = compression;
            return this;
        }

        public Builder<K, V> connectionState(ConnectionState state) {
            this.state = state;
            return this;
        }

        public Builder<K, V> connectionTimeout(long timeout, TimeUnit unit) {
            this.timeout = TimeUnit.SECONDS.convert(timeout, unit);
            return this;
        }

        public Builder<K, V> connectionPool(ManagedConnectionPool pool) {
            this.pool = pool;
            return this;
        }

        public Builder<K, V> accessToken(String accessToken) {
            this.token = accessToken;
            return this;
        }

        public RestSender<K, V> build() {
            Objects.requireNonNull(kafkaConfig);
            Objects.requireNonNull(retriever);
            Objects.requireNonNull(keyEncoder);
            Objects.requireNonNull(valueEncoder);
            if (timeout <= 0) {
                throw new IllegalStateException("Connection timeout must be strictly positive");
            }
            ConnectionState useState = state;
            if (useState == null) {
                useState = new ConnectionState(timeout, TimeUnit.SECONDS);
            }
            ManagedConnectionPool usePool = pool;
            if (usePool == null) {
                usePool = ManagedConnectionPool.GLOBAL_POOL;
            }
            return new RestSender<>(new RestClient(kafkaConfig, timeout, usePool),
                    retriever, keyEncoder, valueEncoder, compression, useState, token);
        }
    }
}

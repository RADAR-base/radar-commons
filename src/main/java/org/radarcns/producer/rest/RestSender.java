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

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.Response;
import org.radarcns.config.ServerConfig;
import org.radarcns.producer.AuthenticationException;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.producer.rest.ConnectionState.State;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.radarcns.producer.rest.RestClient.responseBody;

/**
 * RestSender sends records to the Kafka REST Proxy. It does so using an Avro JSON encoding. A new
 * sender must be constructed with {@link #sender(AvroTopic)} per AvroTopic. This implementation is
 * blocking and unbuffered, so flush, clear and close do not do anything.
 */
public class RestSender implements KafkaSender {
    private static final Logger logger = LoggerFactory.getLogger(RestSender.class);

    public static final String KAFKA_REST_ACCEPT_ENCODING =
            "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json";
    public static final String KAFKA_REST_ACCEPT_LEGACY_ENCODING =
            "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json";
    public static final MediaType KAFKA_REST_AVRO_ENCODING =
            MediaType.parse("application/vnd.kafka.avro.v2+json; charset=utf-8");
    public static final MediaType KAFKA_REST_AVRO_LEGACY_ENCODING =
            MediaType.parse("application/vnd.kafka.avro.v1+json; charset=utf-8");
    private RequestProperties requestProperties;

    private Request.Builder isConnectedRequest;
    private SchemaRetriever schemaRetriever;
    private RestClient httpClient;
    private final ConnectionState state;

    /**
     * Construct a RestSender.
     * @param httpClient client to send requests with
     * @param schemaRetriever non-null Retriever of avro schemas
     * @param useCompression use compression to send data
     * @param sharedState shared connection state
     * @param additionalHeaders headers to add to requests
     */
    private RestSender(RestClient httpClient, SchemaRetriever schemaRetriever,
            boolean useCompression, ConnectionState sharedState, Headers additionalHeaders) {
        this.schemaRetriever = schemaRetriever;
        this.requestProperties = new RequestProperties(KAFKA_REST_ACCEPT_ENCODING,
                KAFKA_REST_AVRO_ENCODING, useCompression, additionalHeaders);
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

    private void setRestClient(RestClient newClient) {
        try {
            isConnectedRequest = newClient.requestBuilder("").head();
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Schemaless topics do not have a valid URL", ex);
        }
        httpClient = newClient;
        state.reset();
    }

    public final synchronized void setSchemaRetriever(SchemaRetriever retriever) {
        this.schemaRetriever = retriever;
    }

    public synchronized RestClient getRestClient() {
        return httpClient;
    }

    public synchronized SchemaRetriever getSchemaRetriever() {
        return this.schemaRetriever;
    }

    private synchronized Request getIsConnectedRequest() {
        return isConnectedRequest.headers(requestProperties.headers).build();
    }

    public synchronized void setCompression(boolean useCompression) {
        this.requestProperties = new RequestProperties(requestProperties.acceptType,
                requestProperties.contentType, useCompression, requestProperties.headers);
    }

    public synchronized Headers getHeaders() {
        return requestProperties.headers;
    }

    public synchronized void setHeaders(Headers additionalHeaders) {
        this.requestProperties = new RequestProperties(requestProperties.acceptType,
                requestProperties.contentType, requestProperties.useCompression, additionalHeaders);
        this.state.reset();
    }

    @Override
    public <K, V> KafkaTopicSender<K, V> sender(AvroTopic<K, V> topic) {
        return new RestTopicSender<>(topic, this, state);
    }

    public synchronized RequestProperties getRequestProperties() {
        return requestProperties;
    }

    @Override
    public boolean resetConnection() throws AuthenticationException {
        if (state.getState() == State.CONNECTED) {
            return true;
        }
        try (Response response = httpClient.request(getIsConnectedRequest())) {
            if (response.isSuccessful()) {
                state.didConnect();
            } else if (response.code() == 401) {
                state.wasUnauthorized();
            } else {
                state.didDisconnect();
                String bodyString = responseBody(response);
                logger.warn("Failed to make heartbeat request to {} (HTTP status code {}): {}",
                        httpClient, response.code(), bodyString);
            }
        } catch (IOException ex) {
            // no stack trace is needed
            state.didDisconnect();
            logger.warn("Failed to make heartbeat request to {}: {}", httpClient, ex.toString());
        }

        if (state.getState() == State.UNAUTHORIZED) {
            throw new AuthenticationException("HEAD request unauthorized");
        }

        return state.getState() == State.CONNECTED;
    }

    public boolean isConnected() throws AuthenticationException {
        switch (state.getState()) {
            case CONNECTED:
                return true;
            case DISCONNECTED:
                return false;
            case UNAUTHORIZED:
                throw new AuthenticationException("Unauthorized");
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

    public synchronized void useLegacyEncoding() {
        this.requestProperties = new RequestProperties(KAFKA_REST_ACCEPT_LEGACY_ENCODING,
                KAFKA_REST_AVRO_LEGACY_ENCODING, requestProperties.useCompression,
                requestProperties.headers);
    }

    public static class Builder {
        private ServerConfig kafkaConfig;
        private SchemaRetriever retriever;
        private boolean compression = false;
        private long timeout = 10;
        private ConnectionState state;
        private ManagedConnectionPool pool;
        private Headers.Builder additionalHeaders = new Headers.Builder();

        public Builder server(ServerConfig kafkaConfig) {
            this.kafkaConfig = kafkaConfig;
            return this;
        }

        public Builder schemaRetriever(SchemaRetriever schemaRetriever) {
            this.retriever = schemaRetriever;
            return this;
        }

        public Builder useCompression(boolean compression) {
            this.compression = compression;
            return this;
        }

        public Builder connectionState(ConnectionState state) {
            this.state = state;
            return this;
        }

        public Builder connectionTimeout(long timeout, TimeUnit unit) {
            this.timeout = TimeUnit.SECONDS.convert(timeout, unit);
            return this;
        }

        public Builder connectionPool(ManagedConnectionPool pool) {
            this.pool = pool;
            return this;
        }

        public Builder headers(Headers headers) {
            additionalHeaders = headers.newBuilder();
            return this;
        }

        public Builder addHeader(String header, String value) {
            additionalHeaders.add(header, value);
            return this;
        }

        public RestSender build() {
            Objects.requireNonNull(kafkaConfig);
            Objects.requireNonNull(retriever);
            if (timeout <= 0) {
                throw new IllegalStateException("Connection timeout must be strictly positive");
            }
            ConnectionState useState;
            ManagedConnectionPool usePool;

            if (state != null) {
                useState = state;
            } else {
                useState = new ConnectionState(timeout, TimeUnit.SECONDS);
            }
            if (pool != null) {
                usePool = pool;
            } else {
                usePool = ManagedConnectionPool.GLOBAL_POOL;
            }

            return new RestSender(new RestClient(kafkaConfig, timeout, usePool),
                    retriever, compression, useState, additionalHeaders.build());
        }
    }

    public static final class RequestProperties {
        public final String acceptType;
        public final MediaType contentType;
        public final boolean useCompression;
        public final Headers headers;

        RequestProperties(String acceptType, MediaType contentType, boolean useCompression,
                Headers headers) {
            this.acceptType = acceptType;
            this.contentType = contentType;
            this.useCompression = useCompression;
            this.headers = headers;
        }
    }
}

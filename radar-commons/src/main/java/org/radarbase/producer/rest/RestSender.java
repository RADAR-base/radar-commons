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

package org.radarbase.producer.rest;

import static org.radarbase.producer.rest.RestClient.DEFAULT_TIMEOUT;
import static org.radarbase.producer.rest.RestClient.responseBody;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.avro.SchemaValidationException;
import org.radarbase.config.ServerConfig;
import org.radarbase.producer.AuthenticationException;
import org.radarbase.producer.KafkaSender;
import org.radarbase.producer.KafkaTopicSender;
import org.radarbase.producer.rest.ConnectionState.State;
import org.radarbase.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static final MediaType KAFKA_REST_BINARY_ENCODING =
            MediaType.parse("application/vnd.radarbase.avro.v1+binary");
    public static final MediaType KAFKA_REST_AVRO_ENCODING =
            MediaType.parse("application/vnd.kafka.avro.v2+json; charset=utf-8");
    public static final MediaType KAFKA_REST_AVRO_LEGACY_ENCODING =
            MediaType.parse("application/vnd.kafka.avro.v1+json; charset=utf-8");
    private RequestProperties requestProperties;

    private Request.Builder connectionTestRequest;
    private SchemaRetriever schemaRetriever;
    private RestClient httpClient;
    private final ConnectionState state;

    /**
     * Construct a RestSender.
     */
    private RestSender(Builder builder) {
        this.schemaRetriever = Objects.requireNonNull(builder.retriever);
        this.requestProperties = new RequestProperties(
                KAFKA_REST_ACCEPT_ENCODING,
                builder.binary ? KAFKA_REST_BINARY_ENCODING : KAFKA_REST_AVRO_ENCODING,
                builder.additionalHeaders.build(),
                builder.binary);
        this.state = builder.state;
        setRestClient(Objects.requireNonNull(builder.client).newBuilder()
                .protocols(Collections.singletonList(Protocol.HTTP_1_1))
                .build());
    }

    /**
     * Set the connection timeout. This affects both the connection state as the HTTP client
     * setting.
     * @param connectionTimeout timeout
     * @param unit time unit
     */
    public synchronized void setConnectionTimeout(long connectionTimeout, TimeUnit unit) {
        if (connectionTimeout != httpClient.getTimeout()) {
            httpClient = httpClient.newBuilder().timeout(connectionTimeout, unit).build();
            state.setTimeout(connectionTimeout, unit);
        }
    }

    /**
     * Set the Kafka REST Proxy settings. This affects the REST client.
     * @param kafkaConfig server configuration of the Kafka REST proxy.
     */
    public synchronized void setKafkaConfig(ServerConfig kafkaConfig) {
        Objects.requireNonNull(kafkaConfig);
        if (kafkaConfig.equals(httpClient.getServer())) {
            return;
        }
        setRestClient(httpClient.newBuilder().server(kafkaConfig).build());
    }

    /**
     * Set the REST client. This will reset the connection state.
     */
    private void setRestClient(RestClient newClient) {
        try {
            connectionTestRequest = newClient.requestBuilder("").head();
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Schemaless topics do not have a valid URL", ex);
        }
        httpClient = newClient;
        state.reset();
    }

    /** Set the schema retriever. */
    public final synchronized void setSchemaRetriever(SchemaRetriever retriever) {
        this.schemaRetriever = retriever;
    }

    /** Get the current REST client. */
    public synchronized RestClient getRestClient() {
        return httpClient;
    }

    /** Get the schema retriever. */
    public synchronized SchemaRetriever getSchemaRetriever() {
        return this.schemaRetriever;
    }

    /** Get a request to check the connection status. */
    private synchronized Request getConnectionTestRequest() {
        return connectionTestRequest.headers(requestProperties.headers).build();
    }

    /** Set the compression of the REST client. */
    public synchronized void setCompression(boolean useCompression) {
        httpClient = httpClient.newBuilder().gzipCompression(useCompression).build();
    }

    /** Get the headers used in requests. */
    public synchronized Headers getHeaders() {
        return requestProperties.headers;
    }

    /** Set the headers used in requests. */
    public synchronized void setHeaders(Headers additionalHeaders) {
        this.requestProperties = new RequestProperties(requestProperties.acceptType,
                requestProperties.contentType, additionalHeaders,
                requestProperties.binary);
        this.state.reset();
    }

    @Override
    public <K, V> KafkaTopicSender<K, V> sender(AvroTopic<K, V> topic)
            throws SchemaValidationException {
        return new RestTopicSender<>(topic, this, state);
    }

    /**
     * Get the current request properties.
     */
    public synchronized RequestProperties getRequestProperties() {
        return requestProperties;
    }

    /**
     * Get the current request context.
     */
    public synchronized RequestContext getRequestContext() {
        return new RequestContext(httpClient, requestProperties);
    }

    @Override
    public boolean resetConnection() throws AuthenticationException {
        if (state.getState() == State.CONNECTED) {
            return true;
        }
        try (Response response = httpClient.request(getConnectionTestRequest())) {
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

    @Override
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
        // noop
    }

    /**
     * Revert to a legacy connection if the server does not support the latest protocols.
     * @param acceptEncoding accept encoding to use in the legacy connection.
     * @param contentEncoding content encoding to use in the legacy connection.
     * @param binary whether to send the data as binary.
     */
    public synchronized void useLegacyEncoding(String acceptEncoding,
            MediaType contentEncoding, boolean binary) {
        logger.debug("Reverting to encoding {} -> {} (binary: {})",
                contentEncoding, acceptEncoding, binary);
        this.requestProperties = new RequestProperties(acceptEncoding,
                contentEncoding,
                requestProperties.headers, binary);
    }

    public static class Builder {
        private SchemaRetriever retriever;
        private ConnectionState state;
        private RestClient client;
        private Headers.Builder additionalHeaders = new Headers.Builder();
        private boolean binary = false;

        public Builder schemaRetriever(SchemaRetriever schemaRetriever) {
            this.retriever = schemaRetriever;
            return this;
        }

        /**
         * Whether to try to send binary content. This only works if the server supports it. If not,
         * there may be an additional round-trip.
         * @param binary true if attempt to send binary content, false otherwise
         */
        public Builder useBinaryContent(boolean binary) {
            this.binary = binary;
            return this;
        }

        /**
         * Whether to try to send binary content. This only works if the server supports it. If not,
         * there may be an additional round-trip.
         * @param binary true if attempt to send binary content, false otherwise
         * @deprecated use {@link #useBinaryContent(boolean)} instead
         */
        @Deprecated
        @SuppressWarnings("PMD.LinguisticNaming")
        public Builder hasBinaryContent(boolean binary) {
            this.binary = binary;
            return this;
        }

        public Builder connectionState(ConnectionState state) {
            this.state = state;
            return this;
        }

        public Builder httpClient(RestClient client) {
            this.client = client;
            return this;
        }

        public Builder headers(Headers headers) {
            additionalHeaders = headers.newBuilder();
            return this;
        }

        public Builder addHeader(String header, String value) {
            additionalHeaders.add(header + ": " + value);
            return this;
        }

        /** Build a new RestSender. */
        public RestSender build() {
            if (state == null) {
                state = new ConnectionState(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
            }

            return new RestSender(this);
        }
    }

    static final class RequestContext {
        final RequestProperties properties;
        final RestClient client;

        RequestContext(RestClient client, RequestProperties properties) {
            this.properties = properties;
            this.client = client;
        }
    }

    static final class RequestProperties {
        final String acceptType;
        final MediaType contentType;
        final Headers headers;
        final boolean binary;

        RequestProperties(String acceptType, MediaType contentType, Headers headers,
                boolean binary) {
            this.acceptType = acceptType;
            this.contentType = contentType;
            this.headers = headers;
            this.binary = binary;
        }
    }
}

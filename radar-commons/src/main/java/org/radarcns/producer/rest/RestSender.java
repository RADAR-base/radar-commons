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
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.avro.SchemaValidationException;
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
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.radarcns.producer.rest.RestClient.DEFAULT_TIMEOUT;
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
    public static final MediaType KAFKA_REST_BINARY_ENCODING =
            MediaType.parse("application/vnd.radarbase.avro.v1+binary");
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

    public synchronized void setConnectionTimeout(long connectionTimeout, TimeUnit unit) {
        if (connectionTimeout != httpClient.getTimeout()) {
            httpClient = httpClient.newBuilder().timeout(connectionTimeout, unit).build();
            state.setTimeout(connectionTimeout, unit);
        }
    }

    public synchronized void setKafkaConfig(ServerConfig kafkaConfig) {
        Objects.requireNonNull(kafkaConfig);
        if (kafkaConfig.equals(httpClient.getServer())) {
            return;
        }
        setRestClient(httpClient.newBuilder().server(kafkaConfig).build());
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
        httpClient = httpClient.newBuilder().gzipCompression(useCompression).build();
    }

    public synchronized Headers getHeaders() {
        return requestProperties.headers;
    }

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

    public synchronized RequestProperties getRequestProperties() {
        return requestProperties;
    }

    public synchronized RequestContext getRequestContext() {
        return new RequestContext(httpClient, requestProperties);
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
        // noop
    }

    public synchronized void useLegacyEncoding(String acceptEncoding,
            MediaType contentEncoding, boolean binary) {
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
            additionalHeaders.add(header, value);
            return this;
        }

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

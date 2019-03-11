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

import static org.radarbase.producer.rest.RestClient.responseBody;
import static org.radarbase.producer.rest.UncheckedRequestException.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.avro.SchemaValidationException;
import org.json.JSONException;
import org.radarbase.data.AvroRecordData;
import org.radarbase.data.RecordData;
import org.radarbase.producer.AuthenticationException;
import org.radarbase.producer.KafkaTopicSender;
import org.radarbase.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RestTopicSender<K, V>
        implements KafkaTopicSender<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(RestTopicSender.class);

    private final AvroTopic<K, V> topic;
    private RecordRequest<K, V> requestData;
    private final RestSender sender;
    private final ConnectionState state;

    RestTopicSender(AvroTopic<K, V> topic, RestSender sender, ConnectionState state)
            throws SchemaValidationException {
        this.topic = topic;
        this.sender = sender;
        this.state = state;

        if (sender.getRequestContext().properties.binary) {
            try {
                requestData = new BinaryRecordRequest<>(topic);
            } catch (IllegalArgumentException ex) {
                logger.warn("Cannot use Binary encoding for incompatible topic {}: {}",
                        topic, ex.toString());
            }
        }

        if (requestData == null) {
            requestData = new JsonRecordRequest<>(topic);
        }
    }

    @Override
    public void send(K key, V value) throws IOException, SchemaValidationException {
        send(new AvroRecordData<>(topic, key, Collections.singletonList(value)));
    }

    /**
     * Actually make a REST request to the Kafka REST server and Schema Registry.
     *
     * @param records values to send
     * @throws IOException if records could not be sent
     */
    @Override
    public void send(RecordData<K, V> records) throws IOException, SchemaValidationException {
        RestSender.RequestContext context = sender.getRequestContext();
        Request request = buildRequest(context, records);

        boolean doResend = false;
        try (Response response = context.client.request(request)) {
            if (response.isSuccessful()) {
                state.didConnect();
                if (logger.isDebugEnabled()) {
                    logger.debug("Added message to topic {} -> {}",
                            topic, responseBody(response));
                }
            } else if (response.code() == 401 || response.code() == 403) {
                state.wasUnauthorized();
            } else if (response.code() == 415) {
                downgradeConnection(request, response);
                doResend = true;
            } else {
                throw fail(request, response, null);
            }
        } catch (IOException ex) {
            state.didDisconnect();
            fail(request, null, ex).rethrow();
        } catch (UncheckedRequestException ex) {
            state.didDisconnect();
            ex.rethrow();
        } finally {
            requestData.reset();
        }

        if (state.getState() == ConnectionState.State.UNAUTHORIZED) {
            throw new AuthenticationException("Request unauthorized");
        }

        if (doResend) {
            send(records);
        }
    }

    private void updateRecords(RestSender.RequestContext context, RecordData<K, V> records)
            throws IOException, SchemaValidationException {
        if (!context.properties.binary && requestData instanceof BinaryRecordRequest) {
            requestData = new JsonRecordRequest<>(topic);
        }

        String sendTopic = topic.getName();
        SchemaRetriever retriever = sender.getSchemaRetriever();

        ParsedSchemaMetadata keyMetadata;
        ParsedSchemaMetadata valueMetadata;

        try {
            keyMetadata = retriever.getOrSetSchemaMetadata(
                    sendTopic, false, topic.getKeySchema(), -1);
            valueMetadata = retriever.getOrSetSchemaMetadata(
                    sendTopic, true, topic.getValueSchema(), -1);
        } catch (JSONException | IOException ex) {
            throw new IOException("Failed to get schemas for topic " + topic, ex);
        }

        requestData.prepare(keyMetadata, valueMetadata, records);
    }

    private void downgradeConnection(Request request, Response response) throws IOException {
        if (this.requestData instanceof BinaryRecordRequest) {
            state.didConnect();
            logger.warn("Binary Avro encoding is not supported."
                    + " Switching to JSON encoding.");
            sender.useLegacyEncoding(
                    RestSender.KAFKA_REST_ACCEPT_ENCODING, RestSender.KAFKA_REST_AVRO_ENCODING,
                    false);
            requestData = new JsonRecordRequest<>(topic);
        } else if (Objects.equals(request.header("Accept"),
                RestSender.KAFKA_REST_ACCEPT_ENCODING)) {
            state.didConnect();
            logger.warn("Latest Avro encoding is not supported. Switching to legacy "
                    + "encoding.");
            sender.useLegacyEncoding(
                    RestSender.KAFKA_REST_ACCEPT_LEGACY_ENCODING, RestSender.KAFKA_REST_AVRO_LEGACY_ENCODING,
                    false);
        } else {
            RequestBody body = request.body();
            MediaType contentType = body != null ? body.contentType() : null;
            if (contentType == null || contentType.equals(RestSender.KAFKA_REST_AVRO_LEGACY_ENCODING)) {
                throw fail(request, response,
                    new IOException("Content-Type " + contentType + " not accepted by server."));
            } else {
                // the connection may have been downgraded already
                state.didConnect();
                logger.warn("Content-Type changed during request");
            }
        }
    }

    private Request buildRequest(RestSender.RequestContext context, RecordData<K, V> records)
            throws IOException, SchemaValidationException {
        updateRecords(context, records);

        HttpUrl sendToUrl = context.client.getRelativeUrl("topics/" + topic.getName());

        TopicRequestBody requestBody;
        Request.Builder requestBuilder = new Request.Builder()
                .url(sendToUrl)
                .headers(context.properties.headers)
                .header("Accept", context.properties.acceptType);

        MediaType contentType = context.properties.contentType;
        if (contentType.equals(RestSender.KAFKA_REST_BINARY_ENCODING)
                && !(requestData instanceof BinaryRecordRequest)) {
            contentType = RestSender.KAFKA_REST_AVRO_ENCODING;
        }
        requestBody = new TopicRequestBody(requestData, contentType);

        return requestBuilder.post(requestBody).build();
    }

    @Override
    public void clear() {
        // nothing
    }

    @Override
    public void flush() {
        // nothing
    }

    @Override
    public void close() {
        // noop
    }
}

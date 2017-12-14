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

import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;
import org.radarcns.data.AvroRecordData;
import org.radarcns.data.Record;
import org.radarcns.data.RecordData;
import org.radarcns.producer.AuthenticationException;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.radarcns.producer.rest.RestClient.responseBody;
import static org.radarcns.producer.rest.RestSender.KAFKA_REST_ACCEPT_ENCODING;
import static org.radarcns.producer.rest.TopicRequestBody.topicRequestContent;

class RestTopicSender<K, V> implements KafkaTopicSender<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(RestTopicSender.class);
    private static final int LOG_CONTENT_LENGTH = 1024;

    private final AvroTopic<K, V> topic;
    private final TopicRequestData requestData;
    private final RestSender sender;
    private final ConnectionState state;

    RestTopicSender(AvroTopic<K, V> topic, RestSender sender, ConnectionState state) {
        this.topic = topic;
        this.sender = sender;
        this.state = state;
        this.requestData = new TopicRequestData();
    }

    @Override
    public void send(K key, V value) throws IOException {
        send(new AvroRecordData<>(topic, Collections.singleton(new Record<>(key, value))));
    }

    /**
     * Actually make a REST request to the Kafka REST server and Schema Registry.
     *
     * @param records values to send
     * @throws IOException if records could not be sent
     */
    @Override
    public void send(RecordData<K, V> records) throws IOException {
        if (records.isEmpty()) {
            return;
        }

        RestClient restClient;
        RestSender.RequestProperties requestProperties;
        synchronized (sender) {
            restClient = sender.getRestClient();
            requestProperties = sender.getRequestProperties();
        }
        Request request = buildRequest(restClient, requestProperties, records);

        boolean doResend = false;
        try (Response response = restClient.request(request)) {
            if (response.isSuccessful()) {
                state.didConnect();
                if (logger.isDebugEnabled()) {
                    logger.debug("Added message to topic {} -> {}",
                            topic, responseBody(response));
                }
            } else if (response.code() == 401) {
                throw new AuthenticationException("Cannot authenticate");
            } else if (response.code() == 403 || response.code() == 422) {
                throw new AuthenticationException("Data does not match authentication");
            } else if (response.code() == 415
                    && Objects.equals(request.header("Accept"), KAFKA_REST_ACCEPT_ENCODING)) {
                state.didConnect();
                logger.warn("Latest Avro encoding is not supported. Switching to legacy "
                        + "encoding.");
                sender.useLegacyEncoding();
                doResend = true;
            } else {
                logFailure(request, response, null);
            }
        } catch (AuthenticationException ex) {
            state.wasUnauthorized();
            throw ex;
        } catch (IOException ex) {
            logFailure(request, null, ex);
        } finally {
            requestData.reset();
        }

        if (doResend) {
            send(records);
        }
    }

    private Request buildRequest(RestClient restClient, RestSender.RequestProperties properties,
            RecordData<K, V> records)
            throws IOException {
        HttpUrl sendToUrl = updateRequestData(restClient, records);

        TopicRequestBody requestBody;
        Request.Builder requestBuilder = new Request.Builder()
                .url(sendToUrl)
                .headers(properties.headers)
                .header("Accept", properties.acceptType);

        if (properties.useCompression) {
            requestBody = new GzipTopicRequestBody(requestData, properties.contentType);
            requestBuilder.addHeader("Content-Encoding", "gzip");
        } else {
            requestBody = new TopicRequestBody(requestData, properties.contentType);
        }

        return requestBuilder.post(requestBody).build();
    }

    private HttpUrl updateRequestData(RestClient restClient, RecordData<K, V> records)
            throws IOException {
        // Get schema IDs
        String sendTopic = topic.getName();

        SchemaRetriever retriever = sender.getSchemaRetriever();
        try {
            ParsedSchemaMetadata metadata = retriever.getOrSetSchemaMetadata(
                    sendTopic, false, topic.getKeySchema(), -1);
            requestData.setKeySchemaId(metadata.getId());
        } catch (IOException ex) {
            throw new IOException("Failed to get schema for key "
                    + topic.getKeyClass().getName() + " of topic " + topic, ex);
        }

        try {
            ParsedSchemaMetadata metadata = retriever.getOrSetSchemaMetadata(
                    sendTopic, true, topic.getValueSchema(), -1);
            requestData.setValueSchemaId(metadata.getId());
        } catch (IOException ex) {
            throw new IOException("Failed to get schema for value "
                    + topic.getValueClass().getName() + " of topic " + topic, ex);
        }
        requestData.setRecords(records);

        return restClient.getRelativeUrl("topics/" + sendTopic);
    }

    @SuppressWarnings("ConstantConditions")
    private void logFailure(Request request, Response response, Exception ex)
            throws IOException {
        state.didDisconnect();
        String content = response == null ? null : responseBody(response);
        int code = response == null ? -1 : response.code();
        String requestContent = topicRequestContent(request);
        if (requestContent != null) {
            requestContent = requestContent.substring(0,
                    Math.min(requestContent.length(), LOG_CONTENT_LENGTH));
        }
        logger.error("FAILED to transmit message: {} -> {}...",
                content, requestContent);
        throw new IOException("Failed to submit (HTTP status code " + code
                + "): " + content, ex);
    }

    @Override
    public void clear() {
        // nothing
    }

    @Override
    public void flush() throws IOException {
        // nothing
    }

    @Override
    public void close() {
        // noop
    }
}

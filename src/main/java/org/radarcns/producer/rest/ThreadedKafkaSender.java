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

import org.radarcns.data.Record;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.RollingTimeAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Send Avro Records to a Kafka REST Proxy.
 *
 * This queues messages for a specified amount of time and then sends all messages up to that time.
 */
public class ThreadedKafkaSender<K, V> implements KafkaSender<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ThreadedKafkaSender.class);
    private static final int RETRIES = 3;
    private static final long HEARTBEAT_TIMEOUT_MILLIS = 60_000L;
    private static final long HEARTBEAT_TIMEOUT_MARGIN = HEARTBEAT_TIMEOUT_MILLIS + 10_000L;

    private final KafkaSender<K, V> wrappedSender;
    private final ScheduledExecutorService executor;
    private final RollingTimeAverage opsSent;
    private final RollingTimeAverage opsRequests;
    private final ConnectionState state;

    /**
     * Create a REST producer that caches some values
     *
     * @param sender Actual KafkaSender
     */
    public ThreadedKafkaSender(KafkaSender<K, V> sender) {
        this.wrappedSender = sender;
        this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread( Runnable r) {
                return new Thread(r, "Kafka REST Producer");
            }
        });
        opsSent = new RollingTimeAverage(20000L);
        opsRequests = new RollingTimeAverage(20000L);
        state = new ConnectionState(HEARTBEAT_TIMEOUT_MARGIN, TimeUnit.MILLISECONDS);

        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                opsRequests.add(1);

                boolean success = sendHeartbeat();
                if (success) {
                    state.didConnect();
                } else {
                    logger.error("Failed to send message");
                    state.didDisconnect();
                }

                if (opsSent.hasAverage() && opsRequests.hasAverage()) {
                    logger.info("Sending {} messages in {} requests per second",
                            (int) Math.round(opsSent.getAverage()),
                            (int) Math.round(opsRequests.getAverage()));
                }
            }
        }, 0L, HEARTBEAT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    private class ThreadedTopicSender<L extends K, W extends V>
            implements KafkaTopicSender<L, W>, Runnable {
        private final KafkaTopicSender<L, W> topicSender;
        private final List<List<Record<L, W>>> topicQueue;
        private final List<List<Record<L, W>>> threadLocalQueue;
        private Future<?> topicFuture;

        private ThreadedTopicSender(AvroTopic<L, W> topic) throws IOException {
            topicSender = wrappedSender.sender(topic);
            topicQueue = new ArrayList<>();
            threadLocalQueue = new ArrayList<>();
            topicFuture = null;
        }

        /**
         * Send given key and record to a topic.
         * @param key key
         * @param value value with schema
         * @throws IOException if the producer is not connected.
         */
        @Override
        public void send(long offset, L key, W value) throws IOException {
            List<Record<L, W>> recordList = new ArrayList<>(1);
            recordList.add(new Record<>(offset, key, value));
            send(recordList);
        }

        @Override
        public synchronized void send(List<Record<L, W>> records) throws IOException {
            if (records.isEmpty()) {
                return;
            }
            if (!isConnected()) {
                throw new IOException("Producer is not connected");
            }
            synchronized (this) {
                topicQueue.add(records);
                if (topicFuture == null) {
                    topicFuture = executor.submit(this);
                }
            }
            notifyAll();
        }

        @Override
        public void clear() {
            synchronized (this) {
                topicFuture.cancel(false);
                topicFuture = null;
                topicQueue.clear();
            }
            topicSender.clear();
        }


        @Override
        public long getLastSentOffset() {
            return this.topicSender.getLastSentOffset();
        }

        @Override
        public void flush() throws IOException {
            Future<?> localFuture = null;
            synchronized (this) {
                if (topicFuture != null) {
                    localFuture = topicFuture;
                }
            }
            if (localFuture != null) {
                try {
                    localFuture.wait();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            topicSender.flush();
        }

        @Override
        public void close() throws IOException {
            flush();
            topicSender.close();
        }

        @Override
        public void run() {
            synchronized (this) {
                threadLocalQueue.addAll(topicQueue);
                topicQueue.clear();
                topicFuture = null;
            }

            opsRequests.add(1);

            for (List<Record<L, W>> records : threadLocalQueue) {
                opsSent.add(records.size());

                IOException exception = null;
                for (int i = 0; i < RETRIES; i++) {
                    try {
                        topicSender.send(records);
                        break;
                    } catch (IOException ex) {
                        exception = ex;
                    }
                }

                if (exception == null) {
                    state.didConnect();
                } else {
                    logger.error("Failed to send message");
                    state.didDisconnect();
                    break;
                }
            }

            threadLocalQueue.clear();
        }
    }

    private boolean sendHeartbeat() {
        boolean success = false;
        for (int i = 0; !success && i < RETRIES; i++) {
            success = wrappedSender.resetConnection();
        }
        return success;
    }

    @Override
    public synchronized boolean isConnected() {
        switch (state.getState()) {
            case CONNECTED:
                return true;
            case DISCONNECTED:
                return false;
            case UNKNOWN:
                state.didDisconnect();
                return false;
            default:
                throw new IllegalStateException("Illegal connection state");
        }
    }

    @Override
    public boolean resetConnection() {
        if (isConnected()) {
            return true;
        } else if (wrappedSender.resetConnection()) {
            state.didConnect();
            return true;
        } else {
            state.didDisconnect();
            return false;
        }
    }

    @Override
    public <L extends K, W extends V> KafkaTopicSender<L, W> sender(AvroTopic<L, W> topic)
            throws IOException {
        return new ThreadedTopicSender<>(topic);
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
    }
}

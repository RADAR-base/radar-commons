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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.data.Record;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.util.Oscilloscope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockDevice<K extends SpecificRecord> extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(MockDevice.class);
    private final int baseFrequency;
    private final KafkaSender<K, SpecificRecord> sender;
    private final AtomicBoolean stopping;
    private final List<RecordGenerator<K>> generators;
    private final K key;

    private IOException exception;

    public MockDevice(KafkaSender<K, SpecificRecord> sender, K key,
            List<RecordGenerator<K>> generators) {
        this.key = key;
        BigInteger gcd = BigInteger.ONE;
        for (RecordGenerator generator : generators) {
            gcd = gcd.gcd(BigInteger.valueOf(generator.getConfig().getFrequency()));
        }
        this.generators = generators;
        baseFrequency = gcd.intValue();
        this.sender = sender;
        this.stopping = new AtomicBoolean(false);
        exception = null;
    }

    public void run() {
        List<KafkaTopicSender<K, SpecificRecord>> topicSenders =
                new ArrayList<>(generators.size());
        List<Iterator<Record<K, SpecificRecord>>> recordIterators =
                new ArrayList<>(generators.size());

        try {
            for (RecordGenerator<K> generator : generators) {
                topicSenders.add(sender.sender(generator.getTopic()));
                recordIterators.add(generator.iterateValues(key, 0));
            }
            Oscilloscope oscilloscope = new Oscilloscope(baseFrequency);

            while (!stopping.get()) {
                // The time keeping is regulated with beats, with baseFrequency beats per second.
                int beat = oscilloscope.beat();

                for (int i = 0; i < generators.size(); i++) {
                    int frequency = generators.get(i).getConfig().getFrequency();
                    if (frequency > 0 && beat % (baseFrequency / frequency) == 0) {
                        Record<K, SpecificRecord> record = recordIterators.get(i).next();
                        topicSenders.get(i).send(record.offset, record.key, record.value);
                    }
                }
            }
        } catch (InterruptedException ex) {
            // do nothing, just exit the loop
        } catch (IOException e) {
            synchronized (this) {
                this.exception = e;
            }
            logger.error("MockDevice {} failed to send message", key, e);
        }
    }

    public void shutdown() {
        stopping.set(true);
    }

    public synchronized IOException getException() {
        return exception;
    }
}

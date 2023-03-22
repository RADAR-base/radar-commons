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
package org.radarbase.mock

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import org.apache.avro.SchemaValidationException
import org.apache.avro.specific.SpecificRecord
import org.radarbase.mock.data.RecordGenerator
import org.radarbase.producer.KafkaSender
import org.radarbase.util.Oscilloscope
import org.slf4j.LoggerFactory
import java.io.IOException
import java.math.BigInteger
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Mock device that sends data for given topics at a given rate. This can be used to simulate
 * any number of real devices.
 * @param <K> record key type
</K> */
class MockDevice<K : SpecificRecord>(
    sender: KafkaSender,
    private val key: K,
    private val generators: List<RecordGenerator<K>>,
) {
    private val baseFrequency: Int
    private val sender: KafkaSender
    private val stopping: AtomicBoolean

    /** Get the exception that occurred in the thread. Returns null if no exception occurred.  */
    @get:Synchronized
    var exception: Exception?
        private set

    /**
     * Basic constructor.
     * @param sender sender to send data with
     * @param key key to send all messages with
     * @param generators data generators that produce the data we send
     */
    init {
        baseFrequency = computeBaseFrequency(generators)
        this.sender = sender
        stopping = AtomicBoolean(false)
        exception = null
    }

    suspend fun CoroutineScope.run() {
        try {
            val topicSenders = generators.map { sender.sender(it.topic) }
            val recordIterators = generators.map { it.iterateValues(key, 0) }
            val oscilloscope = Oscilloscope(baseFrequency)
            try {
                while (isActive) {
                    // The time keeping is regulated with beats, with baseFrequency beats per
                    // second.
                    val beat = oscilloscope.beat()
                    for (i in generators.indices) {
                        val frequency = generators[i].config.frequency
                        if (frequency > 0 && beat % (baseFrequency / frequency) == 0) {
                            val record = recordIterators[i].next()
                            topicSenders[i].send(record.key, record.value)
                        }
                    }
                }
            } catch (ex: InterruptedException) {
                // do nothing, just exit the loop
            }
        } catch (e: SchemaValidationException) {
            synchronized(this) { exception = e }
            logger.error("MockDevice {} failed to send message", key, e)
        } catch (e: IOException) {
            synchronized(this) { exception = e }
            logger.error("MockDevice {} failed to send message", key, e)
        }
    }

    /**
     * Shut down the device eventually.
     */
    fun shutdown() {
        stopping.set(true)
    }

    /** Check whether an exception occurred, and rethrow the exception if that is the case.  */
    @Synchronized
    @Throws(IOException::class, SchemaValidationException::class)
    fun checkException() {
        when (exception) {
            null -> {}
            is IOException -> throw exception as IOException
            is SchemaValidationException -> throw exception as SchemaValidationException
            is RuntimeException -> throw exception as RuntimeException
            else -> throw IllegalStateException("Unknown exception occurred", exception)
        }
    }

    private fun computeBaseFrequency(generators: List<RecordGenerator<K>>): Int {
        var lcm = BigInteger.ONE
        for (generator in generators) {
            val freq = BigInteger.valueOf(generator.config.frequency.toLong())
            lcm = lcm.multiply(freq.divide(lcm.gcd(freq))) // a * (b / gcd(a, b));
        }
        return lcm.toInt()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MockDevice::class.java)
    }
}

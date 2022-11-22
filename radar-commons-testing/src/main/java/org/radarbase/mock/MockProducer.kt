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

import com.opencsv.exceptions.CsvValidationException
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.auth.*
import io.ktor.client.plugins.auth.providers.*
import io.ktor.client.request.forms.*
import io.ktor.http.*
import kotlinx.coroutines.*
import org.apache.avro.SchemaValidationException
import org.radarbase.config.ServerConfig
import org.radarbase.config.YamlConfigLoader
import org.radarbase.management.auth.ClientCredentialsConfig
import org.radarbase.management.auth.clientCredentials
import org.radarbase.mock.config.AuthConfig
import org.radarbase.mock.config.BasicMockConfig
import org.radarbase.mock.config.MockDataConfig
import org.radarbase.mock.data.MockCsvParser
import org.radarbase.mock.data.RecordGenerator
import org.radarbase.producer.KafkaSender
import org.radarbase.producer.io.GzipContentEncoding
import org.radarbase.producer.io.timeout
import org.radarbase.producer.rest.ConnectionState
import org.radarbase.producer.rest.RestKafkaSender.Companion.restKafkaSender
import org.radarbase.producer.schema.SchemaRetriever
import org.radarbase.producer.schema.SchemaRetriever.Companion.schemaRetriever
import org.radarbase.util.TimeoutConfig
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.exitProcess

/**
 * A Mock Producer class that can be used to stream data. It can use MockFileSender and MockDevice
 * for testing purposes, with direct or indirect streaming.
 * @param mockConfig configuration to mock
 * @param root root directory of where mock files are located
 * @throws IOException if the data could not be read or sent
 */
class MockProducer @JvmOverloads constructor(
    mockConfig: BasicMockConfig,
    root: Path? = null,
) {
    private var devices: MutableList<MockDevice<ObservationKey>>
    private var files: MutableList<MockFileSender>
    private val senders: List<KafkaSender>
    private val retriever: SchemaRetriever
    private var job: Job? = null

    init {
        val numDevices = mockConfig.numberOfDevices
        retriever = schemaRetriever(mockConfig.schemaRegistry.urlString) {
            httpClient {
                timeout(Duration.ofSeconds(10))
            }
        }
        val dataConfigs = mockConfig.data
            ?: defaultDataConfig()
        try {
            val generators: List<RecordGenerator<ObservationKey>> = createGenerators(dataConfigs)
            val mockFiles: List<MockCsvParser> = createMockFiles(dataConfigs, root)
            senders = createSenders(
                mockConfig, numDevices + mockFiles.size,
                mockConfig.authConfig
            )

            devices = ArrayList(numDevices)
            files = ArrayList(numDevices)

            if (generators.isNotEmpty()) {
                val userId = "UserID_"
                val sourceId = "SourceID_"
                for (i in 0 until numDevices) {
                    val key = ObservationKey("test", userId + i, sourceId + i)
                    devices.add(MockDevice(senders[i], key, generators))
                }
            }
            for (i in mockFiles.indices) {
                files.add(MockFileSender(senders[i + numDevices], mockFiles[i]))
            }
        } catch (ex: CsvValidationException) {
            throw IOException("Cannot read CSV file", ex)
        } catch (ex: Exception) {
            throw ex
        }
    }

    @Throws(IOException::class)
    private fun createSenders(
        mockConfig: BasicMockConfig, numDevices: Int, authConfig: AuthConfig
    ): List<KafkaSender> = createRestSenders(
        numDevices,
        retriever,
        mockConfig.restProxy,
        mockConfig.hasCompression(),
        authConfig
    )

    /** Create senders that produce data to Kafka via the REST proxy.  */
    @Throws(IOException::class)
    private fun createRestSenders(
        numDevices: Int,
        retriever: SchemaRetriever,
        restProxy: ServerConfig,
        useCompression: Boolean,
        authConfig: AuthConfig?
    ): List<KafkaSender> {
        val sharedState = ConnectionState(TimeoutConfig(Duration.ofSeconds(10)))
        return (0 until numDevices)
            .map {
                restKafkaSender {
                    schemaRetriever = retriever
                    connectionState = sharedState

                    httpClient {
                        defaultRequest {
                            url(restProxy.urlString)
                        }
                        if (authConfig != null) {
                            install(Auth) {
                                clientCredentials(
                                    ClientCredentialsConfig(
                                        authConfig.tokenUrl,
                                        authConfig.clientId,
                                        authConfig.clientSecret
                                    ).copyWithEnv(),
                                    restProxy.host,
                                )
                            }
                        }
                        if (useCompression) {
                            install(GzipContentEncoding)
                        }
                        timeout(Duration.ofSeconds(10))
                    }
                }
            }
    }

    /** Start sending data.  */
    @Throws(IOException::class)
    suspend fun start() {
        job = SupervisorJob()
        withContext(Dispatchers.Default + job!!) {
            for (device in devices) {
                launch {
                    with(device) {
                        run()
                    }
                }
            }
            for (file in files) {
                launch {
                    file.send()
                    logger.info("Sent data {}", file)
                }
            }
        }
        job = null
    }

    /** Stop sending data and clean up all resources.  */
    @Throws(IOException::class, InterruptedException::class, SchemaValidationException::class)
    suspend fun shutdown() {
        job?.run {
            logger.info("Shutting down mock devices")
            cancel()
            join()
        }
        for (device in devices) {
            device.checkException()
        }
    }

    private fun defaultDataConfig(): List<MockDataConfig> {
        val acceleration = MockDataConfig()
        acceleration.topic = "android_empatica_e4_acceleration"
        acceleration.frequency = 32
        acceleration.valueSchema = EmpaticaE4Acceleration::class.java.name
        acceleration.setInterval(-2.0, 2.0)
        acceleration.valueFields = listOf("x", "y", "z")
        val battery = MockDataConfig()
        battery.topic = "android_empatica_e4_battery_level"
        battery.valueSchema = EmpaticaE4BatteryLevel::class.java.name
        battery.frequency = 1
        battery.setInterval(0.0, 1.0)
        battery.setValueField("batteryLevel")
        val bvp = MockDataConfig()
        bvp.topic = "android_empatica_e4_blood_volume_pulse"
        bvp.valueSchema = EmpaticaE4BloodVolumePulse::class.java.name
        bvp.frequency = 64
        bvp.setInterval(60.0, 90.0)
        bvp.setValueField("bloodVolumePulse")
        val eda = MockDataConfig()
        eda.topic = "android_empatica_e4_electrodermal_activity"
        eda.valueSchema = EmpaticaE4ElectroDermalActivity::class.java.name
        eda.setValueField("electroDermalActivity")
        eda.frequency = 4
        eda.setInterval(0.01, 0.05)
        val ibi = MockDataConfig()
        ibi.topic = "android_empatica_e4_inter_beat_interval"
        ibi.valueSchema = EmpaticaE4InterBeatInterval::class.java.name
        ibi.setValueField("interBeatInterval")
        ibi.frequency = 1
        ibi.setInterval(40.0, 150.0)
        val temperature = MockDataConfig()
        temperature.topic = "android_empatica_e4_temperature"
        temperature.valueSchema = EmpaticaE4Temperature::class.java.name
        temperature.frequency = 4
        temperature.setInterval(20.0, 60.0)
        temperature.setValueField("temperature")
        return listOf(acceleration, battery, bvp, eda, ibi, temperature)
    }

    private fun createGenerators(
        configs: List<MockDataConfig>
    ): List<RecordGenerator<ObservationKey>> = configs.mapNotNull { config ->
        if (config.dataFile == null) {
            RecordGenerator(config, ObservationKey::class.java)
        } else {
            null
        }
    }

    @Throws(IOException::class, CsvValidationException::class)
    private fun createMockFiles(
        configs: List<MockDataConfig>,
        dataRoot: Path?
    ): List<MockCsvParser> {
        val now = Instant.now()
        var parent = dataRoot
        if (parent == null) {
            parent = Paths.get(".").toAbsolutePath()
        }
        return configs.mapNotNull { config ->
            if (config.dataFile != null) {
                logger.info("Reading mock data from {}", config.dataFile)
                MockCsvParser(config, parent, now, retriever)
            } else {
                logger.info("Generating mock data from {}", config)
                null
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MockProducer::class.java)

        /**
         * Runs the MockProducer with given YAML mock config file.
         */
        @JvmStatic
        fun main(args: Array<String>) {
            if (args.size != 1) {
                logger.error("This command needs a mock file argument")
                exitProcess(1)
            }
            val mockFile = Paths.get(args[0]).toAbsolutePath()
            val config: BasicMockConfig
            try {
                config = YamlConfigLoader().load(mockFile, BasicMockConfig::class.java)
            } catch (ex: IOException) {
                logger.error("Failed to load given mock file {}: {}", mockFile, ex.message)
                exitProcess(1)
            }
            try {
                val producer = MockProducer(config, mockFile.parent)
                runBlocking {
                    producer.start()
                }
                if (producer.devices.isNotEmpty()) {
                    waitForProducer(producer, config.duration)
                }
            } catch (ex: IllegalArgumentException) {
                logger.error("{}", ex.message)
                exitProcess(1)
            } catch (e: InterruptedException) {
                // during shutdown, not that important. Will shutdown again.
            } catch (ex: Exception) {
                logger.error("Failed to start mock producer", ex)
                exitProcess(1)
            }
        }

        /** Wait for given duration and then stop the producer.  */
        @Throws(IOException::class, InterruptedException::class, SchemaValidationException::class)
        private fun waitForProducer(producer: MockProducer, duration: Long) {
            val isShutdown = AtomicBoolean(false)
            Runtime.getRuntime().addShutdownHook(Thread {
                try {
                    if (!isShutdown.get()) {
                        runBlocking {
                            producer.shutdown()
                        }
                    }
                } catch (ex: InterruptedException) {
                    logger.warn("Shutdown interrupted", ex)
                } catch (ex: Exception) {
                    logger.warn("Failed to shutdown producer", ex)
                }
            })
            if (duration <= 0L) {
                try {
                    logger.info("Producing data until interrupted")
                    Thread.sleep(Long.MAX_VALUE)
                } catch (ex: InterruptedException) {
                    // this is intended
                }
            } else {
                try {
                    logger.info("Producing data for {} seconds", duration / 1000.0)
                    Thread.sleep(duration)
                } catch (ex: InterruptedException) {
                    logger.warn("Data producing interrupted")
                }
                runBlocking {
                    producer.shutdown()
                }
                isShutdown.set(true)
                logger.info("Producing data done.")
            }
        }
    }
}

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
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.defaultRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.avro.SchemaValidationException
import org.radarbase.config.ServerConfig
import org.radarbase.config.YamlConfigLoader
import org.radarbase.ktor.auth.ClientCredentialsConfig
import org.radarbase.ktor.auth.clientCredentials
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
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4Acceleration
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse
import org.radarcns.passive.empatica.EmpaticaE4ElectroDermalActivity
import org.radarcns.passive.empatica.EmpaticaE4InterBeatInterval
import org.radarcns.passive.empatica.EmpaticaE4Temperature
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.seconds

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
    private val job: Job = SupervisorJob()

    init {
        val numDevices = mockConfig.numberOfDevices
        val schemaRetrieverUrl = requireNotNull(mockConfig.schemaRegistry?.urlString) { "Missing schema retriever URL in config" }
        retriever = schemaRetriever(schemaRetrieverUrl) {
            httpClient {
                timeout(10.seconds)
            }
        }
        val dataConfigs = mockConfig.data
            ?: defaultDataConfig()
        try {
            val generators: List<RecordGenerator<ObservationKey>> = createGenerators(dataConfigs)
            val mockFiles: List<MockCsvParser> = createMockFiles(dataConfigs, root)
            senders = createSenders(
                mockConfig,
                numDevices + mockFiles.size,
                requireNotNull(mockConfig.authConfig) { "Missing authentication information in config" },
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
        mockConfig: BasicMockConfig,
        numDevices: Int,
        authConfig: AuthConfig,
    ): List<KafkaSender> = createRestSenders(
        numDevices,
        retriever,
        requireNotNull(mockConfig.restProxy) { "Missing REST Proxy in config" },
        mockConfig.hasCompression(),
        authConfig,
    )

    /** Create senders that produce data to Kafka via the REST proxy.  */
    @Throws(IOException::class)
    private fun createRestSenders(
        numDevices: Int,
        retriever: SchemaRetriever,
        restProxy: ServerConfig,
        useCompression: Boolean,
        authConfig: AuthConfig?,
    ): List<KafkaSender> {
        val scope = CoroutineScope(job)
        val sharedState = ConnectionState(10.seconds, scope)
        return (0 until numDevices)
            .map {
                restKafkaSender {
                    this.scope = scope
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
                                        requireNotNull(authConfig.tokenUrl) { "Missing authentication token URL in config" },
                                        authConfig.clientId,
                                        authConfig.clientSecret,
                                    ).copyWithEnv(),
                                    restProxy.host,
                                )
                            }
                        }
                        if (useCompression) {
                            install(GzipContentEncoding)
                        }
                        timeout(10.seconds)
                    }
                }
            }
    }

    /** Start sending data.  */
    @Throws(IOException::class)
    suspend fun start() {
        withContext(Dispatchers.Default + job) {
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
    }

    /** Stop sending data and clean up all resources.  */
    @Throws(IOException::class, InterruptedException::class, SchemaValidationException::class)
    suspend fun shutdown() {
        logger.info("Shutting down mock devices")
        job.run {
            cancel()
            join()
        }
        for (device in devices) {
            device.checkException()
        }
    }

    private fun defaultDataConfig(): List<MockDataConfig> {
        val acceleration = MockDataConfig().apply {
            topic = "android_empatica_e4_acceleration"
            frequency = 32
            valueSchema = EmpaticaE4Acceleration::class.java.name
            setInterval(-2.0, 2.0)
            valueFields = listOf("x", "y", "z")
        }
        val battery = MockDataConfig().apply {
            topic = "android_empatica_e4_battery_level"
            valueSchema = EmpaticaE4BatteryLevel::class.java.name
            frequency = 1
            setInterval(0.0, 1.0)
            setValueField("batteryLevel")
        }
        val bvp = MockDataConfig().apply {
            topic = "android_empatica_e4_blood_volume_pulse"
            valueSchema = EmpaticaE4BloodVolumePulse::class.java.name
            frequency = 64
            setInterval(60.0, 90.0)
            setValueField("bloodVolumePulse")
        }
        val eda = MockDataConfig().apply {
            topic = "android_empatica_e4_electrodermal_activity"
            valueSchema = EmpaticaE4ElectroDermalActivity::class.java.name
            setValueField("electroDermalActivity")
            frequency = 4
            setInterval(0.01, 0.05)
        }
        val ibi = MockDataConfig().apply {
            topic = "android_empatica_e4_inter_beat_interval"
            valueSchema = EmpaticaE4InterBeatInterval::class.java.name
            setValueField("interBeatInterval")
            frequency = 1
            setInterval(40.0, 150.0)
        }
        val temperature = MockDataConfig().apply {
            topic = "android_empatica_e4_temperature"
            valueSchema = EmpaticaE4Temperature::class.java.name
            frequency = 4
            setInterval(20.0, 60.0)
            setValueField("temperature")
        }
        return listOf(acceleration, battery, bvp, eda, ibi, temperature)
    }

    private fun createGenerators(
        configs: List<MockDataConfig>,
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
        dataRoot: Path?,
    ): List<MockCsvParser> {
        val now = Instant.now()
        val parent = dataRoot ?: Paths.get(".").toAbsolutePath()
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
            Runtime.getRuntime().addShutdownHook(
                Thread {
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
                },
            )
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

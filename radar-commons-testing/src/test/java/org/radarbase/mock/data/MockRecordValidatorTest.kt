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
package org.radarbase.mock.data

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.Mockito.mock
import org.radarbase.mock.config.MockDataConfig
import org.radarbase.producer.schema.SchemaRetriever
import org.radarcns.kafka.ObservationKey
import org.radarcns.monitor.application.ApplicationServerStatus
import org.radarcns.passive.phone.PhoneAcceleration
import org.radarcns.passive.phone.PhoneLight
import java.io.IOException
import java.io.Writer
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.bufferedWriter

class MockRecordValidatorTest {
    @TempDir
    lateinit var folder: Path
    private lateinit var root: Path
    private lateinit var retriever: SchemaRetriever

    @Throws(IOException::class)
    private fun makeConfig(): MockDataConfig {
        return makeConfig(folder)
    }

    @BeforeEach
    fun setUp(@TempDir folder: Path) {
        root = folder.root
        retriever = mock(SchemaRetriever::class.java)
    }

    @Test
    @Throws(IOException::class)
    fun validateEnum() = runTest {
        val config = makeConfig()
        config.valueSchema = ApplicationServerStatus::class.java.name
        withContext(Dispatchers.IO) {
            config.getDataFile(root).bufferedWriter().use { writer ->
                writer.append("key.projectId,key.userId,key.sourceId,value.time,value.serverStatus,value.ipAddress\n")
                writer.append("test,a,b,1,UNKNOWN,\n")
                writer.append("test,a,b,2,CONNECTED,\n")
            }
        }
        MockRecordValidator(config, 2000L, root, retriever).validate()
    }

    @Test
    @Throws(IOException::class)
    fun validateEnumGenerated() = runTest {
        val config = makeConfig()
        config.valueSchema = ApplicationServerStatus::class.java.name
        config.setValueField("serverStatus")
        val generator = CsvGenerator()
        generator.generate(config, 2000L, root)
        MockRecordValidator(config, 2000L, root, retriever).validate()
    }

    @Test
    @Throws(Exception::class)
    fun validate() = runTest {
        val generator = CsvGenerator()
        val config = makeConfig()
        generator.generate(config, 100000L, root)
        MockRecordValidator(config, 100000L, root, retriever).validate()
    }

    @Test
    @Throws(Exception::class)
    fun validateWrongDuration() = runTest {
        val generator = CsvGenerator()
        val config = makeConfig()
        generator.generate(config, 100000L, root)
        assertValidateThrows<IllegalArgumentException>(config)
    }

    @Test
    @Throws(Exception::class)
    fun validateCustom() = runTest {
        val config = writeConfig {
            append("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.light\n")
            append("test,a,b,1,1,1\n")
            append("test,a,b,1,2,1\n")
        }
        assertValidate(config)
    }

    @Test
    @Throws(Exception::class)
    fun validateWrongKey() = runTest {
        val config = writeConfig {
            append("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.light\n")
            append("test,a,b,1,1,1\n")
            append("test,a,c,1,2,1\n")
        }
        assertValidateThrows<IllegalArgumentException>(config)
    }

    @Test
    @Throws(Exception::class)
    fun validateWrongTime() = runTest {
        val config = writeConfig {
            append("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.light\n")
            append("test,a,b,1,1,1\n")
            append("test,a,b,1,0,1\n")
        }
        assertValidateThrows<IllegalArgumentException>(config)
    }

    @Test
    @Throws(Exception::class)
    fun validateMissingKeyField() = runTest {
        val config = writeConfig {
            append("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.light\n")
            append("test,a,1,1,1\n")
            append("test,a,1,2,1\n")
        }
        assertValidateThrows<IllegalArgumentException>(config)
    }

    @Test
    @Throws(Exception::class)
    fun validateMissingValueField() = runTest {
        val config = writeConfig {
            append("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.light\n")
            append("test,a,b,1,1\n")
            append("test,a,b,1,2\n")
        }
        assertValidateThrows<IllegalArgumentException>(config)
    }

    @Test
    @Throws(Exception::class)
    fun validateMissingValue() = runTest {
        val config = writeConfig {
            append("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.light\n")
            append("test,a,b,1,1\n")
            append("test,a,b,1,2,1\n")
        }
        assertValidateThrows<IllegalArgumentException>(config)
    }

    @Test
    @Throws(Exception::class)
    fun validateWrongValueType() = runTest {
        val config = writeConfig {
            append("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.light\n")
            append("test,a,b,1,1,a\n")
            append("test,a,b,1,2,b\n")
        }
        assertValidateThrows<NumberFormatException>(config)
    }

    @Test
    @Throws(Exception::class)
    fun validateMultipleFields() = runTest {
        val config = writeConfig {
            append("key.projectId,key.userId,key.sourceId,value.time,value.timeReceived,value.x,value.y,value.z\n")
            append("test,a,b,1,1,1,1,1\n")
            append("test,a,b,1,2,1,1,1\n")
        }
        config.valueSchema = PhoneAcceleration::class.java.name
        config.valueFields = listOf("x", "y", "z")
        assertValidate(config)
    }

    private suspend inline fun <reified T : Throwable> assertValidateThrows(config: MockDataConfig) {
        val validator = MockRecordValidator(config, 2000L, root, retriever)
        try {
            validator.validate()
            throw AssertionError("No exception thrown (expected ${T::class.java})")
        } catch (ex: Throwable) {
            if (!ex.javaClass.isAssignableFrom(T::class.java)) {
                throw AssertionError("Another exception than ${T::class.java} thrown", ex)
            }
        }
    }

    private suspend fun writeConfig(write: Writer.() -> Unit): MockDataConfig {
        val config = makeConfig()
        withContext(Dispatchers.IO) {
            config.getDataFile(root).bufferedWriter().use(write)
        }
        return config
    }

    private suspend fun assertValidate(config: MockDataConfig) {
        MockRecordValidator(config, 2000L, root, retriever).validate()
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun makeConfig(folder: Path): MockDataConfig {
            val config = MockDataConfig()
            val dataFile = Files.createTempFile(folder, "datafile", ".csv")
            config.keySchema = ObservationKey::class.java.name
            config.dataFile = dataFile.toAbsolutePath().toString()
            config.valueSchema = PhoneLight::class.java.name
            config.setValueField("light")
            config.topic = "test"
            return config
        }
    }
}

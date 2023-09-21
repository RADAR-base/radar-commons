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

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.greaterThan
import org.hamcrest.Matchers.lessThanOrEqualTo
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.radarbase.mock.config.MockDataConfig
import org.radarbase.mock.data.RecordGenerator
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4Acceleration

/**
 * Created by joris on 17/05/2017.
 */
class RecordGeneratorTest {
    @Test
    fun generate() {
        val config = MockDataConfig()
        config.topic = "test"
        config.frequency = 10
        config.minimum = 0.1
        config.maximum = 9.9
        config.valueFields = mutableListOf("x", "y", "z")
        config.valueSchema = EmpaticaE4Acceleration::class.java.getName()
        val generator = RecordGenerator(
            config,
            ObservationKey::class.java,
        )
        val iter = generator
            .iterateValues(ObservationKey("test", "a", "b"), 0)
        val record = iter.next()
        assertEquals(ObservationKey("test", "a", "b"), record.key)
        val x = (record.value as EmpaticaE4Acceleration).x
        assertTrue(x >= 0.1f && x < 9.9f)
        val y = (record.value as EmpaticaE4Acceleration).x
        assertTrue(y >= 0.1f && y < 9.9f)
        val z = (record.value as EmpaticaE4Acceleration).x
        assertTrue(z >= 0.1f && z < 9.9f)
        val time = (record.value as EmpaticaE4Acceleration).time
        val now = System.currentTimeMillis()
        assertThat(time, greaterThan(now / 1000.0 - 1.0))
        assertThat(time, lessThanOrEqualTo(now / 1000.0))
        val nextRecord = iter.next()
        assertEquals(time + 0.1, (nextRecord.value[0] as Double), 1e-6)
    }

    @Test
    fun getHeaders() {
        val config = MockDataConfig()
        config.topic = "test"
        config.valueSchema = EmpaticaE4Acceleration::class.java.getName()
        val generator = RecordGenerator(
            config,
            ObservationKey::class.java,
        )
        assertArrayEquals(
            arrayOf("key.projectId", "key.userId", "key.sourceId", "value.time", "value.timeReceived", "value.x", "value.y", "value.z"),
            generator.headerArray,
        )
    }
}

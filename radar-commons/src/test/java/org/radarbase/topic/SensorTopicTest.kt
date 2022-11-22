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
package org.radarbase.topic

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneAcceleration

/**
 * Created by joris on 05/07/2017.
 */
class SensorTopicTest {
    @Test
    fun workingConstructor() {
        val keySchema = SchemaBuilder.record("key").fields()
            .name("projectId").type(
                Schema.createUnion(
                    Schema.create(Schema.Type.NULL), Schema.create(
                        Schema.Type.STRING
                    )
                )
            ).withDefault(null)
            .name("userId").type(Schema.create(Schema.Type.STRING)).noDefault()
            .name("sourceId").type(Schema.create(Schema.Type.STRING)).noDefault()
            .endRecord()
        val valueSchema = SchemaBuilder.record("value").fields()
            .name("time").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .name("timeReceived").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .name("value").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .endRecord()
        SensorTopic(
            "test",
            keySchema, valueSchema,
            GenericRecord::class.java, GenericRecord::class.java
        )
    }

    @Test
    fun missingUserId() {
        val keySchema = SchemaBuilder.record("key").fields()
            .name("sourceId").type(Schema.create(Schema.Type.STRING)).noDefault()
            .endRecord()
        val valueSchema = SchemaBuilder.record("value").fields()
            .name("time").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .name("timeReceived").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .name("value").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .endRecord()

        assertThrows<IllegalArgumentException> {
            SensorTopic(
                "test",
                keySchema, valueSchema,
                GenericRecord::class.java, GenericRecord::class.java
            )
        }
    }

    @Test
    fun missingTime() {
        val keySchema = SchemaBuilder.record("key").fields()
            .name("userId").type(Schema.create(Schema.Type.STRING)).noDefault()
            .name("sourceId").type(Schema.create(Schema.Type.STRING)).noDefault()
            .endRecord()
        val valueSchema = SchemaBuilder.record("value").fields()
            .name("timeReceived").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .name("value").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .endRecord()
        assertThrows<IllegalArgumentException> {
            SensorTopic(
                "test",
                keySchema, valueSchema,
                GenericRecord::class.java, GenericRecord::class.java
            )
        }
    }

    @Test
    fun notARecord() {
        val keySchema = Schema.create(Schema.Type.STRING)
        val valueSchema = SchemaBuilder.record("value").fields()
            .name("timeReceived").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .name("value").type(Schema.create(Schema.Type.DOUBLE)).noDefault()
            .endRecord()
        assertThrows<IllegalArgumentException> {
            SensorTopic(
                "test",
                keySchema, valueSchema,
                GenericRecord::class.java, GenericRecord::class.java
            )
        }
    }

    @Test
    fun parseTopic() {
        val topic: SensorTopic<ObservationKey, PhoneAcceleration> = SensorTopic.parse(
            "test",
            ObservationKey::class.java.name, PhoneAcceleration::class.java.name
        )
        val expected = SensorTopic(
            "test",
            ObservationKey.getClassSchema(), PhoneAcceleration.getClassSchema(),
            ObservationKey::class.java, PhoneAcceleration::class.java
        )
        assertEquals(expected, topic)
    }

    @Test
    fun parseUnexistingKey() {
        assertThrows<IllegalArgumentException> {
            SensorTopic.parse<ObservationKey, PhoneAcceleration>(
                "test",
                "unexisting." + ObservationKey::class.java.name,
                PhoneAcceleration::class.java.name
            )
        }
    }

    @Test
    fun parseUnexistingValue() {
        assertThrows<IllegalArgumentException> {
            SensorTopic.parse<ObservationKey, PhoneAcceleration>(
                "test",
                ObservationKey::class.java.name,
                "unexisting." + PhoneAcceleration::class.java.name
            )
        }
    }
}

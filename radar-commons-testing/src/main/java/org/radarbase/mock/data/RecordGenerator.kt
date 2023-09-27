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

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type.DOUBLE
import org.apache.avro.Schema.Type.ENUM
import org.apache.avro.Schema.Type.FLOAT
import org.apache.avro.Schema.Type.INT
import org.apache.avro.Schema.Type.LONG
import org.apache.avro.specific.SpecificRecord
import org.radarbase.data.Record
import org.radarbase.mock.config.MockDataConfig
import org.radarbase.topic.AvroTopic
import org.radarbase.util.Metronome
import java.util.EnumSet
import java.util.concurrent.ThreadLocalRandom
import kotlin.random.Random

/**
 * Generates records according to the specification in a [MockDataConfig].
 * Given key class must match the one specified in the config.
 *
 * @param K type of key to generate
 * @param config configuration to use
 */
open class RecordGenerator<K : SpecificRecord>(
    val config: MockDataConfig,
    keyClass: Class<K>,
) {
    val topic: AvroTopic<K, SpecificRecord> = config.parseAvroTopic()
    private val timeField: Field
    private val timeReceivedField: Field?
    private val valueFields: List<Field>
    private val unknownFields: List<Field>
    private val header: List<String>

    init {
        // doing type checking below.
        require(topic.keyClass == keyClass) {
            "RecordGenerator only generates ObservationKey keys, not ${topic.keyClass} in topic $topic"
        }
        require(SpecificRecord::class.java.isAssignableFrom(topic.valueClass)) {
            "RecordGenerator only generates SpecificRecord values, not ${topic.valueClass} in topic $topic"
        }
        header = ArrayList()
        header.addAll(mutableListOf("projectId", "userId", "sourceId"))

        // cache key and value fields
        val valueSchema = topic.valueSchema
        timeField = forceGetField(valueSchema, "time")
        timeReceivedField = valueSchema.getField("timeReceived")

        val valueFieldNames = config.valueFields ?: emptyList()
        valueFields = valueFieldNames
            .map { fieldName -> forceGetField(valueSchema, fieldName) }
            .onEach { field ->
                val type = field.schema().type
                require(ACCEPTABLE_VALUE_TYPES.contains(type)) {
                    "Cannot generate data for type $type in field ${field.name()} in topic $topic"
                }
            }

        unknownFields = ArrayList(valueSchema.fields.size - valueFields.size - 2)
        val existingNames = buildSet(valueFieldNames.size + 2) {
            add("time")
            add("timeReceived")
            addAll(valueFieldNames)
        }
        for (field in valueSchema.fields) {
            header.add(field.name())
            if (field.name() !in existingNames) {
                unknownFields.add(field)
            }
        }
    }

    /**
     * Get the header with correct prefixes.
     * @return generated header
     */
    val headerArray: Array<String> = Array(header.size) { idx ->
        val name = header[idx]
        if (idx <= 2) {
            "key.$name"
        } else {
            "value.$name"
        }
    }

    /** Get given schema field, and throw an IllegalArgumentException if it does not exists.  */
    private fun forceGetField(
        schema: Schema,
        name: String,
    ): Field = requireNotNull(schema.getField(name)) {
        "Schema for topic $topic does not contain required field $name"
    }

    /**
     * Simulates data of a sensor with the given frequency for a time interval specified by
     * duration. The data is converted to lists of strings.
     * @param duration in milliseconds for the simulation
     * @param key key to generate data with
     * @return list containing simulated values
     */
    open fun iteratableRawValues(key: K, duration: Long): Iterable<Array<String>> = iterateValues(key, duration)
        .asSequence()
        .map { record ->
            val keyFieldsSize = record.key.schema.fields.size
            val valueFieldsSize = record.value.schema.fields.size
            Array(keyFieldsSize + valueFieldsSize) { idx ->
                if (idx < keyFieldsSize) {
                    record.key[idx]
                } else {
                    record.value[idx - keyFieldsSize]
                }.toString()
            }
        }
        .asIterable()

    /**
     * Simulates data of a sensor with the given frequency for a time interval specified by
     * duration.
     * @param duration in milliseconds for the simulation
     * @param key key to generate data with
     * @return list containing simulated values
     */
    fun iterateValues(key: K, duration: Long): Iterator<Record<K, SpecificRecord>> = Metronome(
        duration * config.frequency / 1000L,
        config.frequency,
    )
        .asSequence()
        .map { time -> Record(key, createValue(time)) }
        .iterator()

    private fun createValue(time: Long) = topic.newValueInstance().apply {
        put(timeField.pos(), time / 1000.0)
        if (timeReceivedField != null) {
            put(timeReceivedField.pos(), getTimeReceived(time) / 1000.0)
        }
        for (f in valueFields) {
            val fieldValue: Any = when (val type = f.schema().type) {
                DOUBLE -> randomDouble
                FLOAT -> randomDouble.toFloat()
                LONG -> randomDouble.toLong()
                INT -> randomDouble.toInt()
                ENUM -> getRandomEnum(f.schema())
                else -> throw IllegalStateException("Cannot parse type $type")
            }
            put(f.pos(), fieldValue)
        }
        for (f in unknownFields) {
            put(f.pos(), f.defaultVal())
        }
    }

    /**
     * Get a random double.
     * @return random `Double` using `ThreadLocalRandom`.
     */
    private val randomDouble: Double
        get() = Random.nextDouble(config.minimum, config.maximum)

    /**
     * It returns the time a message is received.
     * @param time time at which the message has been sent
     * @return random `Double` representing the Round Trip Time for the given timestamp
     * using `ThreadLocalRandom`
     */
    private fun getTimeReceived(time: Long): Long = time + Random.nextLong(1, 10)

    companion object {
        private val ACCEPTABLE_VALUE_TYPES: Set<Type> = EnumSet.of(DOUBLE, FLOAT, INT, LONG, ENUM)

        private fun getRandomEnum(schema: Schema): Any {
            return try {
                val cls = Class.forName(schema.fullName)
                val values = cls.getMethod("values")

                @Suppress("UNCHECKED_CAST")
                val symbols = values.invoke(null) as Array<Any>
                val symbolIndex = ThreadLocalRandom.current().nextInt(symbols.size)
                symbols[symbolIndex]
            } catch (e: ReflectiveOperationException) {
                throw IllegalArgumentException(
                    "Cannot generate random enum class " + schema.fullName,
                    e,
                )
            } catch (e: ClassCastException) {
                throw IllegalArgumentException(
                    "Cannot generate random enum class " + schema.fullName,
                    e,
                )
            }
        }
    }
}

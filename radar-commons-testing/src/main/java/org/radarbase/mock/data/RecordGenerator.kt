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
 *
 * @param <K> type of key to generate
</K> */
open class RecordGenerator<K : SpecificRecord>(val config: MockDataConfig, keyClass: Class<K>) {
    val topic: AvroTopic<K, SpecificRecord>
    private val timeField: Field
    private val timeReceivedField: Field?
    private val valueFields: MutableList<Field>
    private val unknownFields: MutableList<Field>
    private val header: MutableList<String>

    /**
     * Generates records according to config. Given key class must match the one specified in the
     * config.
     * @param config configuration to use
     */
    init {

        // doing type checking below.
        topic = config.parseAvroTopic()
        require(topic.keyClass == keyClass) {
            (
                "RecordGenerator only generates ObservationKey keys, not " +
                    topic.keyClass + " in topic " + topic
                )
        }
        require(SpecificRecord::class.java.isAssignableFrom(topic.valueClass)) {
            (
                "RecordGenerator only generates SpecificRecord values, not " +
                    topic.valueClass + " in topic " + topic
                )
        }
        header = ArrayList()
        header.addAll(mutableListOf("projectId", "userId", "sourceId"))

        // cache key and value fields
        val valueSchema = topic.valueSchema
        timeField = forceGetField(valueSchema, "time")
        timeReceivedField = valueSchema.getField("timeReceived")
        var valueFieldNames = config.valueFields
        if (valueFieldNames == null) {
            valueFieldNames = emptyList()
        }
        valueFields = ArrayList(valueFieldNames.size)
        for (fieldName in valueFieldNames) {
            val field = forceGetField(valueSchema, fieldName)
            valueFields.add(field)
            val type = field.schema().type
            require(ACCEPTABLE_VALUE_TYPES.contains(type)) {
                (
                    "Cannot generate data for type " + type +
                        " in field " + fieldName + " in topic " + topic
                    )
            }
        }
        unknownFields = ArrayList(valueSchema.fields.size - valueFields.size - 2)
        for (field in valueSchema.fields) {
            header.add(field.name())
            if (field.name() == "time" || field.name() == "timeReceived" || valueFieldNames.contains(
                    field.name(),
                )
            ) {
                continue
            }
            unknownFields.add(field)
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
    open fun iteratableRawValues(key: K, duration: Long): Iterable<Array<String>> {
        return Iterable {
            val baseIterator = iterateValues(key, duration)
            RecordArrayIterator(baseIterator)
        }
    }

    /**
     * Simulates data of a sensor with the given frequency for a time interval specified by
     * duration.
     * @param duration in milliseconds for the simulation
     * @param key key to generate data with
     * @return list containing simulated values
     */
    fun iterateValues(key: K, duration: Long): Iterator<Record<K, SpecificRecord>> {
        return RecordIterator(duration, key)
    }

    private val randomDouble: Double
        /**
         * Get a random double.
         * @return random `Double` using `ThreadLocalRandom`.
         */
        get() = Random.nextDouble(config.minimum, config.maximum)

    /**
     * It returns the time a message is received.
     * @param time time at which the message has been sent
     * @return random `Double` representing the Round Trip Time for the given timestamp
     * using `ThreadLocalRandom`
     */
    private fun getTimeReceived(time: Long): Long {
        return time + Random.nextLong(1, 10)
    }

    private class RecordArrayIterator<K : SpecificRecord>(
        private val baseIterator: Iterator<Record<K, SpecificRecord>>,
    ) : MutableIterator<Array<String>> {
        override fun hasNext(): Boolean = baseIterator.hasNext()

        override fun next(): Array<String> {
            val record = baseIterator.next()
            val keyFieldsSize = record.key.schema.fields.size
            val valueFieldsSize = record.value.schema.fields.size
            return Array(keyFieldsSize + valueFieldsSize) { idx ->
                if (idx < keyFieldsSize) {
                    record.key[idx]
                } else {
                    record.value[idx - keyFieldsSize]
                }.toString()
            }
        }

        override fun remove() {
            throw UnsupportedOperationException("remove")
        }
    }

    private inner class RecordIterator(duration: Long, private val key: K) :
        MutableIterator<Record<K, SpecificRecord>> {
        private val timestamps: Metronome

        init {
            timestamps = Metronome(
                duration * config.frequency / 1000L,
                config.frequency,
            )
        }

        override fun hasNext(): Boolean {
            return timestamps.hasNext()
        }

        override fun next(): Record<K, SpecificRecord> {
            check(hasNext()) { "Iterator done" }
            val value = topic.newValueInstance()
            val time = timestamps.next()
            value.put(timeField.pos(), time / 1000.0)
            if (timeReceivedField != null) {
                value.put(timeReceivedField.pos(), getTimeReceived(time) / 1000.0)
            }
            for (f in valueFields) {
                val type = f.schema().type
                var fieldValue: Any?
                when (type) {
                    DOUBLE -> fieldValue = randomDouble
                    FLOAT -> fieldValue = randomDouble.toFloat()
                    LONG -> fieldValue = randomDouble.toLong()
                    INT -> fieldValue = randomDouble.toInt()
                    ENUM -> fieldValue = getRandomEnum(f.schema())
                    else -> throw IllegalStateException("Cannot parse type $type")
                }
                value.put(f.pos(), fieldValue)
            }
            for (f in unknownFields) {
                value.put(f.pos(), f.defaultVal())
            }
            return Record(key, value)
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }

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

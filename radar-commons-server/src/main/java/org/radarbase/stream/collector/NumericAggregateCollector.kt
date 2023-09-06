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
package org.radarbase.stream.collector

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificRecord
import org.radarbase.util.SpecificAvroConvertible
import java.math.BigDecimal
import java.math.BigDecimal.valueOf
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.*

/**
 * Java class to aggregate data using Kafka Streams. Double is the base type.
 * Only the sum and sorted history are collected, other getSamples are calculated on request.
 */
class NumericAggregateCollector @JvmOverloads constructor(
    var name: String? = null,
    schema: Schema? = null,
    useReservoir: Boolean = false
) : RecordCollector, SpecificAvroConvertible {
    private var pos = 0
    private var fieldType: Schema.Type? = null
    var min: Double
        private set
    var max: Double
        private set
    var count: Long = 0
        private set
    private var sum: BigDecimal
    var reservoir: UniformSamplingReservoir?
        private set

    /** Aggregate collector with only a field name.  */
    constructor(fieldName: String?, useReservoir: Boolean) : this(fieldName, null, useReservoir)
    /** Aggregate collector with a field name and accompanying schema containing the name.  */
    /** Aggregate collector with only a field name.  */
    init {
        sum = BigDecimal.ZERO
        min = Double.POSITIVE_INFINITY
        max = Double.NEGATIVE_INFINITY
        reservoir = if (useReservoir) UniformSamplingReservoir() else null
        if (schema == null) {
            pos = -1
            fieldType = null
        } else {
            val field = schema.getField(name)
                ?: throw IllegalArgumentException(
                    "Field " + name + " does not exist in schema " + schema.fullName
                )
            pos = field.pos()
            fieldType = getType(field)
        }
    }

    override fun add(record: IndexedRecord): NumericAggregateCollector {
        check(pos != -1) { "Cannot add record without specifying a schema in the constructor." }
        val value = record[pos] as? Number ?: return this
        return if (fieldType == Schema.Type.FLOAT) {
            add(value as Float)
        } else {
            add(value.toDouble())
        }
    }

    /** Add a single sample.  */
    fun add(value: Float): NumericAggregateCollector {
        return add(value.toString().toDouble())
    }

    /**
     * Add a sample to the collection.
     * @param value new sample that has to be analysed
     */
    fun add(value: Double): NumericAggregateCollector {
        sum = sum.add(value.toBigDecimal())
        reservoir?.add(value)
        if (value > max) {
            max = value
        }
        if (value < min) {
            min = value
        }
        count++
        return this
    }

    override fun toString(): String {
        return ("DoubleValueCollector{"
                + "name=" + name
                + ", min=" + min
                + ", max=" + max
                + ", sum=" + getSum()
                + ", mean=" + mean
                + ", quartile=" + quartile
                + ", count=" + count
                + ", reservoir=" + reservoir + '}')
    }

    fun getSum(): Double {
        return sum.toDouble()
    }

    val mean: Double
        get() = sum.toDouble() / count

    /** Has a sampling reservoir.  */
    fun hasReservoir(): Boolean = reservoir != null

    val quartile: List<Double>
        /**
         * Get the quartiles as estimated from a uniform sampling reservoir.
         * @throws IllegalStateException if the collector does not keep a sampling reservoir, as
         * indicated by [.hasReservoir].
         */
        get() = checkNotNull(reservoir) { "Cannot query quartiles without reservoir" }
            .quartiles
    val interQuartileRange: Double
        /**
         * Difference between the first quartile and third quartile (IQR).
         * @throws IllegalStateException if the collector does not keep a sampling reservoir, as
         * indicated by [.hasReservoir].
         */
        get() {
            val quartiles = quartile
            return valueOf(quartiles[2])
                .subtract(valueOf(quartiles[0])).toDouble()
        }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as NumericAggregateCollector
        return pos == other.pos &&
            count == other.count &&
            min == other.min &&
            max == other.max &&
            name == other.name &&
            fieldType == other.fieldType &&
            sum == other.sum &&
            reservoir == other.reservoir
    }

    override fun hashCode(): Int {
        return Objects.hash(name, pos, fieldType, min, max, sum, reservoir)
    }

    override fun toAvro(): SpecificRecord {
        val state = NumericAggregateState()
        state.count = count
        if (count > 0) {
            state.min = min
            state.max = max
            state.sum = BigDecimalState(
                ByteBuffer.wrap(sum.unscaledValue().toByteArray()),
                sum.scale()
            )
        } else {
            state.min = null
            state.max = null
            state.sum = null
        }
        if (pos != -1) {
            state.pos = pos
            state.fieldType = fieldType!!.name
        } else {
            state.pos = null
            state.fieldType = null
        }
        state.name = name
        state.reservoir = reservoir?.toAvro()
        return state
    }

    override fun fromAvro(record: SpecificRecord) {
        require(record is NumericAggregateState) { "Cannot deserialize from non NumericAggregateState" }
        name = record.name
        if (record.pos != null) {
            pos = record.pos
            fieldType = Schema.Type.valueOf(record.fieldType)
        } else {
            pos = -1
            fieldType = null
        }
        count = record.count
        if (count > 0) {
            min = record.min
            max = record.max
            sum = BigDecimal(
                BigInteger(record.sum.intVal.array()),
                record.sum.scale
            )
        } else {
            min = Double.MAX_VALUE
            max = Double.MIN_VALUE
            sum = BigDecimal.ZERO
        }
        reservoir = if (record.reservoir == null) {
            null
        } else {
            UniformSamplingReservoir(DoubleArray(0), 0, 1).apply {
                fromAvro(record.reservoir)
            }
        }
    }

    companion object {
        /**
         * Get the non-null number type for a given field. If the tye is a union, it will use the first
         * non-null type in the union.
         * @param field record field to get type for.
         * @return type
         * @throws IllegalArgumentException if the resulting field is non-numeric.
         */
        private fun getType(field: Schema.Field): Schema.Type {
            var apparentType = field.schema().type
            if (apparentType == Schema.Type.UNION) {
                val unionType = field.schema().types
                    .firstOrNull { it.type != Schema.Type.NULL }
                if (unionType != null) {
                    apparentType = unionType.type
                }
            }
            require(
                apparentType == Schema.Type.DOUBLE ||
                apparentType == Schema.Type.FLOAT ||
                apparentType == Schema.Type.INT ||
                apparentType == Schema.Type.LONG) {
                "Field " + field.name() + " is not a number type."
            }
            return apparentType
        }
    }
}

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

/**
 * Java class to aggregate multiple fields of data using Kafka Streams. It can be used for example
 * on acceleration or gyroscope observations that have multiple axes.
 */
class AggregateListCollector : RecordCollector, SpecificAvroConvertible {
    private var collectors: Array<NumericAggregateCollector>?

    constructor() {
        collectors = null
    }

    /** Array collector without schema. Double entries can be added, but entire records cannot.  */
    constructor(fieldNames: Array<String>, useReservoir: Boolean) : this(
        fieldNames,
        null,
        useReservoir,
    )

    /**
     * Aggregate list collector with single record schema. Double entries or records can be added.
     * This assumes that all fields in the aggregate list are extracted from a single record schema.
     */
    constructor(fieldNames: Array<String>, schema: Schema?, useReservoir: Boolean) {
        collectors = Array(fieldNames.size) { i ->
            NumericAggregateCollector(fieldNames[i], schema, useReservoir)
        }
    }

    override fun add(record: IndexedRecord): AggregateListCollector {
        collectors?.forEach { it.add(record) }
        return this
    }

    /**
     * Add a sample to the collection.
     * @param value new sample that has to be analysed
     */
    fun add(vararg value: Double): AggregateListCollector {
        val collectors = collectors ?: return this
        require(collectors.size == value.size) {
            (
                "The length of current input differs from the length of the value used to " +
                    "instantiate this collector"
                )
        }
        for (i in collectors.indices) {
            collectors[i].add(value[i])
        }
        return this
    }

    override fun toString(): String = collectors.contentToString()

    fun getCollectors(): List<NumericAggregateCollector> {
        return collectors?.toList()
            ?: listOf()
    }

    override fun toAvro(): AggregateListState {
        val collectors = collectors
        return AggregateListState().apply {
            aggregates = collectors
                ?.map { it.toAvro() }
                ?: emptyList()
        }
    }

    override fun fromAvro(record: SpecificRecord) {
        require(record is AggregateListState) { "Cannot convert incompatible Avro record" }
        val aggregates = record.aggregates
        val len = aggregates.size
        collectors = Array(len) { i ->
            val aggregate = aggregates[i]
            require(aggregate is NumericAggregateState) { "Cannot convert type " + record.javaClass }
            NumericAggregateCollector().apply {
                fromAvro(aggregate)
            }
        }
    }
}

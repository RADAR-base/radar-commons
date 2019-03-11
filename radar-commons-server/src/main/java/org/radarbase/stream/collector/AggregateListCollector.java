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

package org.radarbase.stream.collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.radarbase.util.SpecificAvroConvertible;

/**
 * Java class to aggregate multiple fields of data using Kafka Streams. It can be used for example
 * on acceleration or gyroscope observations that have multiple axes.
 */
public class AggregateListCollector implements RecordCollector, SpecificAvroConvertible {
    private NumericAggregateCollector[] collectors;

    public AggregateListCollector() {
        this.collectors = null;
    }

    /** Array collector without schema. Double entries can be added, but entire records cannot. */
    public AggregateListCollector(String[] fieldNames, boolean useReservoir) {
        this(fieldNames, null, useReservoir);
    }

    /**
     * Aggregate list collector with single record schema. Double entries or records can be added.
     * This assumes that all fields in the aggregate list are extracted from a single record schema.
     */
    public AggregateListCollector(String[] fieldNames, Schema schema, boolean useReservoir) {
        collectors = new NumericAggregateCollector[fieldNames.length];
        for (int i = 0; i < collectors.length; i++) {
            collectors[i] = new NumericAggregateCollector(fieldNames[i], schema, useReservoir);
        }
    }

    @Override
    public AggregateListCollector add(SpecificRecord record) {
        for (NumericAggregateCollector collector : collectors) {
            collector.add(record);
        }
        return this;
    }

    /**
     * Add a sample to the collection.
     * @param value new sample that has to be analysed
     */
    public AggregateListCollector add(double... value) {
        if (collectors.length != value.length) {
            throw new IllegalArgumentException(
                    "The length of current input differs from the length of the value used to "
                            + "instantiate this collector");
        }
        for (int i = 0; i < collectors.length; i++) {
            collectors[i].add(value[i]);
        }

        return this;
    }

    @Override
    public String toString() {
        return Arrays.toString(collectors);
    }

    public List<NumericAggregateCollector> getCollectors() {
        return Arrays.asList(collectors);
    }

    @Override
    public AggregateListState toAvro() {
        AggregateListState state = new AggregateListState();
        if (collectors == null) {
            state.setAggregates(Collections.emptyList());
        } else {
            List<Object> aggregates = new ArrayList<>(collectors.length);
            for (NumericAggregateCollector collector : collectors) {
                aggregates.add(collector.toAvro());
            }
            state.setAggregates(aggregates);
        }
        return state;
    }

    @Override
    public void fromAvro(SpecificRecord record) {
        if (!(record instanceof AggregateListState)) {
            throw new IllegalArgumentException("Cannot convert incompatible Avro record");
        }

        AggregateListState aggregateList = (AggregateListState) record;
        List<Object> aggregates = aggregateList.getAggregates();
        int len = aggregates.size();

        collectors = new NumericAggregateCollector[len];
        for (int i = 0; i < len; i++) {
            Object aggregate = aggregates.get(i);
            if (aggregate instanceof NumericAggregateState) {
                collectors[i] = new NumericAggregateCollector();
            } else {
                throw new IllegalArgumentException("Cannot convert type " + record.getClass());
            }
            collectors[i].fromAvro((SpecificRecord) aggregate);
        }
    }
}

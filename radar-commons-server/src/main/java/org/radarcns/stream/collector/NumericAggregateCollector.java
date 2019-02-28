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

package org.radarcns.stream.collector;

import static org.radarcns.util.Serialization.floatToDouble;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.util.SpecificAvroConvertible;

/**
 * Java class to aggregate data using Kafka Streams. Double is the base type.
 * Only the sum and sorted history are collected, other getSamples are calculated on request.
 */
public class NumericAggregateCollector implements RecordCollector, SpecificAvroConvertible {
    private String name;
    private int pos;
    private Type fieldType;
    private double min;
    private double max;
    private long count;
    private BigDecimal sum;
    private UniformSamplingReservoir reservoir;

    /** Aggregate collector with only a field name. */
    public NumericAggregateCollector() {
        this(null, null, false);
    }

    /** Aggregate collector with only a field name. */
    public NumericAggregateCollector(String fieldName, boolean useReservoir) {
        this(fieldName, null, useReservoir);
    }

    /** Aggregate collector with a field name and accompanying schema containing the name. */
    public NumericAggregateCollector(String fieldName, Schema schema, boolean useReservoir) {
        sum = BigDecimal.ZERO;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
        count = 0;
        reservoir = useReservoir ? new UniformSamplingReservoir() : null;

        name = fieldName;
        if (schema == null) {
            pos = -1;
            fieldType = null;
        } else {
            Field field = schema.getField(fieldName);
            if (field == null) {
                throw new IllegalArgumentException(
                        "Field " + fieldName + " does not exist in schema " + schema.getFullName());
            }
            pos = field.pos();
            fieldType = getType(field);
        }
    }

    /**
     * Get the non-null number type for a given field. If the tye is a union, it will use the first
     * non-null type in the union.
     * @param field record field to get type for.
     * @return type
     * @throws IllegalArgumentException if the resulting field is non-numeric.
     */
    private static Type getType(Field field) {
        Type apparentType = field.schema().getType();
        if (apparentType == Type.UNION) {
            for (Schema subSchema : field.schema().getTypes()) {
                if (subSchema.getType() != Type.NULL) {
                    apparentType = subSchema.getType();
                    break;
                }
            }
        }

        if (apparentType != Type.DOUBLE
                && apparentType != Type.FLOAT
                && apparentType != Type.INT
                && apparentType != Type.LONG) {
            throw new IllegalArgumentException("Field " + field.name() + " is not a number type.");
        }

        return apparentType;
    }

    @Override
    public NumericAggregateCollector add(SpecificRecord record) {
        if (pos == -1) {
            throw new IllegalStateException(
                    "Cannot add record without specifying a schema in the constructor.");
        }
        Number value = (Number)record.get(pos);
        if (value == null) {
            return this;
        }
        if (fieldType == Type.FLOAT) {
            return add(Double.parseDouble(value.toString()));
        } else {
            return add(value.doubleValue());
        }
    }

    /** Add a single sample. */
    public NumericAggregateCollector add(float value) {
        return this.add(floatToDouble(value));
    }

    /**
     * Add a sample to the collection.
     * @param value new sample that has to be analysed
     */
    public NumericAggregateCollector add(double value) {
        sum = sum.add(BigDecimal.valueOf(value));
        if (reservoir != null) {
            reservoir.add(value);
        }
        if (value > max) {
            max = value;
        }
        if (value < min) {
            min = value;
        }
        count++;

        return this;
    }

    @Override
    public String toString() {
        return "DoubleValueCollector{"
                + "name=" + getName()
                + ", min=" + getMin()
                + ", max=" + getMax()
                + ", sum=" + getSum()
                + ", mean=" + getMean()
                + ", quartile=" + getQuartile()
                + ", count=" + getCount()
                + ", reservoir=" + reservoir + '}';
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getSum() {
        return sum.doubleValue();
    }

    public long getCount() {
        return count;
    }

    public double getMean() {
        return sum.doubleValue() / getCount();
    }

    /** Has a sampling reservoir. */
    public boolean hasReservoir() {
        return reservoir != null;
    }

    /**
     * Get the quartiles as estimated from a uniform sampling reservoir.
     * @throws IllegalStateException if the collector does not keep a sampling reservoir, as
     *                               indicated by {@link #hasReservoir()}.
     */
    public List<Double> getQuartile() {
        if (!hasReservoir()) {
            throw new IllegalStateException("Cannot query quartiles without reservoir");
        }
        return reservoir.getQuartiles();
    }

    /**
     * Difference between the first quartile and third quartile (IQR).
     * @throws IllegalStateException if the collector does not keep a sampling reservoir, as
     *                               indicated by {@link #hasReservoir()}.
     */
    public double getInterQuartileRange() {
        List<Double> quartiles = getQuartile();
        return BigDecimal.valueOf(quartiles.get(2))
                .subtract(BigDecimal.valueOf(quartiles.get(0))).doubleValue();
    }

    public String getName() {
        return name;
    }

    protected UniformSamplingReservoir getReservoir() {
        return reservoir;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NumericAggregateCollector that = (NumericAggregateCollector) o;
        return pos == that.pos
                && count == that.count
                && Double.compare(that.min, min) == 0
                && Double.compare(that.max, max) == 0
                && Objects.equals(name, that.name)
                && fieldType == that.fieldType
                && Objects.equals(sum, that.sum)
                && Objects.equals(reservoir, that.reservoir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, pos, fieldType, min, max, sum, reservoir);
    }

    @Override
    public SpecificRecord toAvro() {
        NumericAggregateState state = new NumericAggregateState();
        state.setCount(count);
        if (count > 0) {
            state.setMin(min);
            state.setMax(max);
            state.setSum(new BigDecimalState(
                    ByteBuffer.wrap(sum.unscaledValue().toByteArray()),
                    sum.scale()));
        } else {
            state.setMin(null);
            state.setMax(null);
            state.setSum(null);
        }
        if (pos != -1) {
            state.setPos(pos);
            state.setFieldType(fieldType.name());
        } else {
            state.setPos(null);
            state.setFieldType(null);
        }
        state.setName(name);
        state.setReservoir(reservoir != null ? reservoir.toAvro() : null);
        return state;
    }

    @Override
    public void fromAvro(SpecificRecord record) {
        if (!(record instanceof NumericAggregateState)) {
            throw new IllegalArgumentException("Cannot deserialize from non NumericAggregateState");
        }
        NumericAggregateState state = (NumericAggregateState) record;

        name = state.getName();
        if (state.getPos() != null) {
            pos = state.getPos();
            fieldType = Schema.Type.valueOf(state.getFieldType());
        } else {
            pos = -1;
            fieldType = null;
        }
        count = state.getCount();

        if (count > 0) {
            min = state.getMin();
            max = state.getMax();
            sum = new BigDecimal(
                    new BigInteger(state.getSum().getIntVal().array()),
                    state.getSum().getScale());
        } else {
            min = Double.MAX_VALUE;
            max = Double.MIN_VALUE;
            sum = BigDecimal.ZERO;
        }

        if (state.getReservoir() == null) {
            reservoir = null;
        } else {
            reservoir = new UniformSamplingReservoir(new double[0], 0, 1);
            reservoir.fromAvro(state.getReservoir());
        }
    }
}

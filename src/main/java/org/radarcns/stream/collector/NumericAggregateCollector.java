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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecord;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

import static org.radarcns.util.Serialization.floatToDouble;

/**
 * Java class to aggregate data using Kafka Streams. Double is the base type.
 * Only the sum and sorted history are collected, other getSamples are calculated on request.
 */
@JsonDeserialize(builder = NumericAggregateCollector.Builder.class)
public class NumericAggregateCollector implements RecordCollector {
    private final String name;
    private final int pos;
    private final Type fieldType;
    private double min;
    private double max;
    private BigDecimal sum;
    private final UniformSamplingReservoir reservoir;

    public NumericAggregateCollector(Builder builder) {
        this.name = builder.nameValue;
        this.pos = builder.posValue;
        this.fieldType = builder.fieldTypeValue;
        this.min = builder.minValue;
        this.max = builder.maxValue;
        this.sum = builder.sumValue;
        this.reservoir = builder.reservoirValue;
    }

    public NumericAggregateCollector(String fieldName) {
        this(fieldName, null);
    }

    public NumericAggregateCollector(String fieldName, Schema schema) {
        sum = BigDecimal.ZERO;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
        reservoir = new UniformSamplingReservoir();

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

    public NumericAggregateCollector add(float value) {
        return this.add(floatToDouble(value));
    }

    /**
     * Add a sample to the collection.
     * @param value new sample that has to be analysed
     */
    public NumericAggregateCollector add(double value) {
        sum = sum.add(BigDecimal.valueOf(value));
        reservoir.add(value);
        if (value > max) {
            max = value;
        }
        if (value < min) {
            min = value;
        }

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

    public int getCount() {
        return reservoir.getCount();
    }

    public double getMean() {
        return sum.doubleValue() / getCount();
    }

    public List<Double> getQuartile() {
        return reservoir.getQuartiles();
    }

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

    @JsonPOJOBuilder(withPrefix = "")
    public static class Builder {
        private double maxValue = Double.NEGATIVE_INFINITY;
        private double minValue = Double.POSITIVE_INFINITY;
        private final String nameValue;
        private int posValue = -1;
        private Type fieldTypeValue = null;
        private BigDecimal sumValue = BigDecimal.ZERO;
        private UniformSamplingReservoir reservoirValue = new UniformSamplingReservoir();

        @JsonCreator
        public Builder(@JsonProperty("name") String name) {
            this.nameValue = Objects.requireNonNull(name);
        }

        @JsonSetter
        public Builder pos(int pos) {
            posValue = pos;
            return this;
        }

        @JsonSetter
        public Builder fieldType(Type fieldType) {
            fieldTypeValue = fieldType;
            return this;
        }

        @JsonSetter
        public Builder min(double min) {
            if (min < minValue) {
                minValue = min;
            }
            return this;
        }

        @JsonSetter
        public Builder max(double max) {
            if (max > maxValue) {
                maxValue = max;
            }
            return this;
        }

        @JsonSetter
        public Builder sum(BigDecimal sum) {
            sumValue = sum;
            return this;
        }

        @JsonSetter
        public Builder reservoir(UniformSamplingReservoir reservoir) {
            this.reservoirValue = reservoir;
            return this;
        }

        @JsonSetter
        public Builder history(List<Double> history) {
            min(history.get(0));
            max(history.get(history.size() - 1));
            reservoir(new UniformSamplingReservoir(history));
            return this;
        }

        public NumericAggregateCollector build() {
            return new NumericAggregateCollector(this);
        }
    }
}

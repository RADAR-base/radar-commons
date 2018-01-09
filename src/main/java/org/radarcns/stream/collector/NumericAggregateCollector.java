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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificRecord;

/** Java class to aggregate data using Kafka Streams. Double is the base type. */
public class NumericAggregateCollector implements RecordCollector {
    private final String name;
    private final int pos;
    private final Type fieldType;
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private BigDecimal sum = BigDecimal.ZERO;
    private int count = 0;
    private double mean = 0;
    private final Double[] quartile = new Double[3];

    private final List<Double> history = new ArrayList<>();

    public NumericAggregateCollector(String fieldName) {
        this(fieldName, null);
    }

    public NumericAggregateCollector(String fieldName, Schema schema) {
        this.name = fieldName;
        if (schema == null) {
            this.pos = -1;
            this.fieldType = null;
        } else {
            Field field = schema.getField(fieldName);
            if (field == null) {
                throw new IllegalArgumentException(
                        "Field " + fieldName + " does not exist in schema " + schema.getFullName());
            }
            this.pos = field.pos();

            Type apparentType = field.schema().getType();
            if (apparentType == Type.UNION) {
                for (Schema subSchema : field.schema().getTypes()) {
                    if (subSchema.getType() != Type.NULL) {
                        apparentType = subSchema.getType();
                        break;
                    }
                }
            }
            fieldType = apparentType;

            if (fieldType != Type.DOUBLE
                    && fieldType != Type.FLOAT
                    && fieldType != Type.INT
                    && fieldType != Type.LONG) {
                throw new IllegalArgumentException("Field " + fieldName + " is not a number type.");
            }
        }
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
            return add(value.floatValue());
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
        updateMin(value);
        updateMax(value);
        updateMean(value);
        updateQuartile(value);

        return this;
    }

    /**
     * @param value new sample that update min value
     */
    private void updateMin(double value) {
        if (min > value) {
            min = value;
        }
    }

    /**
     * @param value new sample that update max value
     */
    private void updateMax(double value) {
        if (max < value) {
            max = value;
        }
    }

    /**
     * @param value new sample that update average value
     */
    private void updateMean(double value) {
        count++;
        // use BigDecimal to avoid loss of precision
        sum = sum.add(BigDecimal.valueOf(value));

        mean = sum.doubleValue() / count;
    }

    /**
     * @param value new sample that update quartiles value
     */
    private void updateQuartile(double value) {
        int index = Collections.binarySearch(history, value);
        if (index >= 0) {
            history.add(index, value);
        } else {
            history.add(-index - 1, value);
        }

        int length = history.size();

        if (length == 1) {
            quartile[0] = quartile[1] = quartile[2] = history.get(0);
        } else {
            for (int i = 0; i < 3; i++) {
                double pos = (i + 1) * (length + 1) / 4.0d;  // == (i + 1) * 25 * (length + 1) / 100
                int intPos = (int) pos;
                if (intPos == 0) {
                    quartile[i] = history.get(0);
                } else if (intPos == length) {
                    quartile[i] = history.get(length - 1);
                } else {
                    double diff = pos - intPos;
                    double base = history.get(intPos - 1);
                    quartile[i] = base + diff * (history.get(intPos) - base);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "DoubleValueCollector{"
                + "name=" + getName()
                + ", min=" + getMin()
                + ", max=" + getMax()
                + ", sum=" + getSum()
                + ", count=" + getCount()
                + ", mean=" + getMean()
                + ", quartile=" + getQuartile()
                + ", history=" + history + '}';
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
        return count;
    }

    public double getMean() {
        return mean;
    }

    public List<Double> getQuartile() {
        return  Arrays.asList(quartile);
    }

    public double getIqr() {
        return BigDecimal.valueOf(quartile[2])
                .subtract(BigDecimal.valueOf(quartile[0])).doubleValue();
    }

    public String getName() {
        return name;
    }
}

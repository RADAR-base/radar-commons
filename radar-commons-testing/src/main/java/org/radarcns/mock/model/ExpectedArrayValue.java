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

package org.radarcns.mock.model;

import java.util.List;
import org.apache.avro.Schema;
import org.radarcns.stream.collector.DoubleArrayCollector;

/**
 * {@code ExpectedValue} represented as {@code Double[]}.
 *
 * {@link ExpectedValue}
 */
public class ExpectedArrayValue extends ExpectedValue<DoubleArrayCollector> {
    /**
     * Constructor.
     */
    public ExpectedArrayValue(Schema valueSchema, List<String> valueFields) {
        super(valueSchema, valueFields);
    }

    public ExpectedArrayValue() {
        super();
    }

    @Override
    protected DoubleArrayCollector createValue() {
        return new DoubleArrayCollector();
    }

    @Override
    protected void addToValue(DoubleArrayCollector collector, Object[] rawValues) {
        double[] values = new double[rawValues.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = Double.parseDouble(rawValues[i].toString());
        }
        collector.add(values);
    }
}

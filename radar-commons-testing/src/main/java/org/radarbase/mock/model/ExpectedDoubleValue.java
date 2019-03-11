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

package org.radarbase.mock.model;

import java.util.List;
import org.apache.avro.Schema;
import org.radarbase.stream.collector.NumericAggregateCollector;

/**
 * {@code ExpectedValue} represented as {@code Double}.
 *
 * {@link ExpectedValue}
 */
public class ExpectedDoubleValue extends ExpectedValue<NumericAggregateCollector> {
    /**
     * Constructor.
     */
    public ExpectedDoubleValue(Schema valueSchema, List<String> valueFields) {
        super(valueSchema, valueFields);
    }

    @Override
    protected NumericAggregateCollector createCollector() {
        return new NumericAggregateCollector(fieldNames[0], schema, true);
    }
}

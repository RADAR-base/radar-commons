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

package org.radarcns.integration.model;

import org.radarcns.key.MeasurementKey;

public class MockRecord {
    private MeasurementKey key;
    private double time = Double.NaN;

    public long getTimeMillis() {
        return (long)(time * 1000d);
    }

    public long getTimeWindow(long intervalMillis) {
        long ms = getTimeMillis();
        return ms - (ms % intervalMillis);
    }

    public MeasurementKey getKey() {
        return key;
    }

    public void setKey(MeasurementKey key) {
        this.key = key;
    }

    public void setTime(double time) {
        this.time = time;
    }

    public static class DoubleType extends MockRecord {
        private double value;

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }
    }

    public static class DoubleArrayType extends MockRecord {
        private double[] values;

        public double[] getValues() {
            return values;
        }

        public void setValues(double[] values) {
            this.values = values;
        }
    }
}

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

package org.radarcns.integration.aggregator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/** Java class to aggregate data using Kafka Streams. Double is the base unit */
public class DoubleValueCollector {
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;
    private double sum = 0.0;
    private double count = 0.0;
    private double avg = 0.0;
    private final double[] quartile = new double[3];
    private double iqr = 0.0;


    private final List<Double> history = new ArrayList<>();

    /**
     * @param value new sample that has to be analysed
     */
    public DoubleValueCollector add(double value) {
        updateMin(value);
        updateMax(value);
        updateAvg(value);
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
    private void updateAvg(double value) {
        count++;
        sum += value;

        avg = sum / count;
    }

    /**
     * @param value new sample that update quartiles value
     */
    private void updateQuartile(double value) {
        history.add(value);
        double[] data = new double[history.size()];
        for (int i = 0; i < history.size(); i++) {
            data[i] = history.get(i);
        }

        DescriptiveStatistics ds = new DescriptiveStatistics(data);

        quartile[0] = ds.getPercentile(25);
        quartile[1] = ds.getPercentile(50);
        quartile[2] = ds.getPercentile(75);

        iqr = quartile[2] - quartile[0];
    }

    @Override
    public String toString() {
        return "DoubleValueCollector{"
                + "min=" + min
                + ", max=" + max
                + ", sum=" + sum
                + ", count=" + count
                + ", avg=" + avg
                + ", quartile=" + Arrays.toString(quartile)
                + ", iqr=" + iqr
                + ", history=" + history + '}';
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getSum() {
        return sum;
    }

    public double getCount() {
        return count;
    }

    public double getAvg() {
        return avg;
    }

    public double[] getQuartile() {
        return quartile;
    }

    public double getIqr() {
        return iqr;
    }
}

package org.radarcns.stream.collector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class UniformSamplingReservoir {
    private final List<Double> samples;
    private final int maxSize;
    private int count;
    private static final int MAX_SIZE_DEFAULT = 999;

    public UniformSamplingReservoir() {
        this(Collections.<Double>emptyList(), 0, MAX_SIZE_DEFAULT);
    }

    public UniformSamplingReservoir(List<Double> allSamples) {
        this(allSamples, allSamples.size(), MAX_SIZE_DEFAULT);
    }

    @JsonCreator
    public UniformSamplingReservoir(
            @JsonProperty("samples") List<Double> samples,
            @JsonProperty("count") int count,
            @JsonProperty("maxSize") int maxSize) {
        this.samples = new ArrayList<>(Objects.requireNonNull(samples));

        if (maxSize <= 0) {
            throw new IllegalArgumentException("Reservoir size must be strictly positive");
        }
        this.maxSize = maxSize;

        if (count < 0) {
            throw new IllegalArgumentException("Reservoir size must be positive");
        }
        this.count = count;


        int toRemove = this.samples.size() - maxSize;
        if (toRemove > 0) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < toRemove; i++) {
                this.samples.remove(random.nextInt(this.samples.size()));
            }
        }
        Collections.sort(this.samples);
    }

    public void add(double value) {
        boolean doAdd;
        int removeIndex;

        if (count < maxSize) {
            doAdd = true;
        } else {
            removeIndex = ThreadLocalRandom.current().nextInt(count);
            if (removeIndex < maxSize) {
                samples.remove(removeIndex);
                doAdd = true;
            } else {
                doAdd = false;
            }
        }

        if (doAdd) {
            int index = Collections.binarySearch(samples, value);
            if (index >= 0) {
                samples.add(index, value);
            } else {
                samples.add(-index - 1, value);
            }
        }

        count++;
    }

    public List<Double> getQuartiles() {
        int length = samples.size();

        List<Double> quartiles;
        if (length == 1) {
            Double elem = samples.get(0);
            quartiles = Arrays.asList(elem, elem, elem);
        } else {
            quartiles = new ArrayList<>(3);
            for (int i = 1; i <= 3; i++) {
                double pos = i * (length + 1) / 4.0d;  // == i * 25 * (length + 1) / 100
                int intPos = (int) pos;
                if (intPos == 0) {
                    quartiles.add(samples.get(0));
                } else if (intPos == length) {
                    quartiles.add(samples.get(length - 1));
                } else {
                    double diff = pos - intPos;
                    double base = samples.get(intPos - 1);
                    quartiles.add(base + diff * (samples.get(intPos) - base));
                }
            }
        }

        return quartiles;
    }

    public List<Double> getSamples() {
        return Collections.unmodifiableList(samples);
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UniformSamplingReservoir that = (UniformSamplingReservoir) o;
        return count == that.count
                && maxSize == that.maxSize
                && Objects.equals(samples, that.samples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(samples, maxSize, count);
    }

    @Override
    public String toString() {
        return "UniformSamplingReservoir{"
                + "samples=" + samples
                + ", maxSize=" + maxSize
                + ", count=" + count
                + '}';
    }
}

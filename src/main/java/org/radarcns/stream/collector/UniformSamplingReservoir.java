package org.radarcns.stream.collector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Uniform sampling reservoir for streaming. This should capture the input distribution in order
 * to compute quartiles, using so-called Algorithm R.
 *
 * <p>The maximum size of the reservoir can be increased to get more accurate quartile estimations.
 * As long as the number of samples is lower than the maximum size of the reservoir, the quartiles
 * are computed exactly.
 */
public class UniformSamplingReservoir {
    private final List<Double> samples;
    private final int maxSize;
    private int count;
    private static final int MAX_SIZE_DEFAULT = 999;

    /** Empty reservoir with default maximum size. */
    public UniformSamplingReservoir() {
        this(Collections.<Double>emptyList(), 0, MAX_SIZE_DEFAULT);
    }

    /**
     * Create a reservoir that samples from given values.
     * @param allValues list of values to sample from.
     * @throws NullPointerException if given allValues are {@code null}.
     */
    public UniformSamplingReservoir(List<Double> allValues) {
        this(allValues, allValues.size(), MAX_SIZE_DEFAULT);
    }

    /**
     * Create a reservoir that samples from given values.
     * @param samples list of values to sample from.
     * @param count current size of the number of samples that the reservoir represents.
     * @param maxSize maximum reservoir size.
     * @throws NullPointerException if given allValues are {@code null}
     */
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

    /** Add a sample to the reservoir. */
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

    /**
     * Get the quartiles of the underlying distribution. If the number of samples is larger than
     * the maximum size of the reservoir, this will be an estimate.
     * @return list with size three, of the 25, 50 and 75 percentiles.
     */
    public List<Double> getQuartiles() {
        int length = samples.size();

        List<Double> quartiles;
        if (length == 1) {
            Double elem = samples.get(0);
            quartiles = Arrays.asList(elem, elem, elem);
        } else {
            quartiles = new ArrayList<>(3);
            for (int i = 1; i <= 3; i++) {
                double pos = i * (length + 1) * 0.25d; // 25 percentile steps
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

    /** Get the currently stored samples. */
    public List<Double> getSamples() {
        return Collections.unmodifiableList(samples);
    }

    /** Get the maximum size of this reservoir. */
    public int getMaxSize() {
        return maxSize;
    }

    /** Get the number of samples that are being represented by the reservoir. */
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

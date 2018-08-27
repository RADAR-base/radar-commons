package org.radarcns.stream.collector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class UniformSamplingReservoirTest {
    @Test
    public void add() {
        UniformSamplingReservoir reservoir = new UniformSamplingReservoir(
                new double[] {0.1, 0.3, 0.5}, 3, 3);
        reservoir.add(0.7);
        assertEquals(3, reservoir.getSamples().size());
        assertEquals(3, reservoir.getMaxSize());
        reservoir.add(0.7);
        assertEquals(3, reservoir.getSamples().size());
        reservoir.add(0.7);
        assertEquals(3, reservoir.getSamples().size());
    }

    @Test
    public void addRandom() {
        UniformSamplingReservoir reservoir = new UniformSamplingReservoir(new double[0], 0, 50);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 100; i++) {
            reservoir.add(random.nextDouble(-1.0, 1.0));
            assertTrue(isOrdered(reservoir.getSamples()));
            assertTrue(reservoir.getSamples().size() <= 50);
        }
        assertEquals(50, reservoir.getSamples().size());
    }

    @Test
    public void addFromRandom() {
        UniformSamplingReservoir reservoir = new UniformSamplingReservoir(new double[0], 0, 50);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        double[] chooseFrom = {-0.1, Double.NEGATIVE_INFINITY, Double.NaN, 1.0};

        for (int i = 0; i < 100; i++) {
            reservoir.add(chooseFrom[random.nextInt(chooseFrom.length)]);
            assertTrue(isOrdered(reservoir.getSamples()));
            assertTrue(reservoir.getSamples().size() <= 50);
        }
        assertEquals(50, reservoir.getSamples().size());
    }

    private static <T extends Comparable<T>> boolean isOrdered(List<T> list) {
        Iterator<T> iterator = list.iterator();
        if (!iterator.hasNext()) {
            return true;
        }
        T previous = iterator.next();
        while (iterator.hasNext()) {
            T current = iterator.next();
            if (previous.compareTo(current) > 0) {
                return false;
            }
            previous = current;
        }
        return true;
    }
}
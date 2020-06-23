package org.radarbase.util;

import java.util.Objects;

public class TimedValue<T> implements TimedVariable {
    public final T value;
    private final long expiry;

    public TimedValue(T value, long validity) {
        expiry = System.currentTimeMillis() + validity * 1000L;
        this.value = Objects.requireNonNull(value);
    }

    @Override
    public boolean isExpired() {
        return expiry < System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimedValue<?> other = (TimedValue<?>)o;
        return value.equals(other.value)
                && expiry == other.expiry;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}

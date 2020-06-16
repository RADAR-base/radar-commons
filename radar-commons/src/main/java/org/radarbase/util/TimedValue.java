package org.radarbase.util;

import java.util.Objects;

public class TimedValue<T> {
    public final T value;
    private final long expiry;

    public TimedValue(T value, long validity) {
        expiry = System.currentTimeMillis() + validity * 1000L;
        this.value = Objects.requireNonNull(value);
    }

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
        return value.equals(((TimedValue<?>)o).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}

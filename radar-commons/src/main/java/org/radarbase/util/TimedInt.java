package org.radarbase.util;

public class TimedInt implements TimedVariable {
    public final int value;
    private final long expiry;

    public TimedInt(int value, long validity) {
        expiry = System.currentTimeMillis() + validity * 1000L;
        this.value = value;
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
        TimedInt other = (TimedInt)o;
        return value == other.value
                && expiry == other.expiry;
    }

    @Override
    public int hashCode() {
        return value;
    }
}

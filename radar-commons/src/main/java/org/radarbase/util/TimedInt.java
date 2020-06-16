package org.radarbase.util;

public class TimedInt {
    public final int value;
    private final long expiry;

    public TimedInt(int value, long validity) {
        expiry = System.currentTimeMillis() + validity * 1000L;
        this.value = value;
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
        return value == ((TimedInt)o).value;
    }

    @Override
    public int hashCode() {
        return value;
    }
}

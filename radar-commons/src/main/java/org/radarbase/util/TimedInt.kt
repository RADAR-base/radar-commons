package org.radarbase.util

class TimedInt(
    val value: Int,
    cacheConfig: TimeoutConfig,
) : TimedVariable(cacheConfig) {
    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        other as TimedInt
        return value == other.value && expiry == other.expiry
    }

    override fun hashCode(): Int = value
}

package org.radarbase.util

class TimedValue<T: Any>(
    val value: T,
    private val cacheConfig: CacheConfig,
) : TimedVariable(cacheConfig) {
    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        other as TimedValue<*>
        return value == other.value && expiry == other.expiry
    }

    override fun hashCode(): Int = value.hashCode()
}

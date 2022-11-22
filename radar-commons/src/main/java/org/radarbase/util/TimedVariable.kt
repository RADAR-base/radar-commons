package org.radarbase.util

open class TimedVariable(
    private val cacheConfig: TimeoutConfig,
) {
    protected val expiry: Long = cacheConfig.currentExpiryTime

    val isExpired: Boolean
        get() = cacheConfig.isPassed(expiry)

    companion object {
        internal fun MutableMap<*, out TimedVariable>.prune() {
            val iterator = values.iterator()
            for (value in iterator) {
                if (value.isExpired) {
                    iterator.remove()
                }
            }
        }
    }
}

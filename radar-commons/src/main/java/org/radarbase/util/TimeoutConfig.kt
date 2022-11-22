package org.radarbase.util

import java.time.Duration

data class TimeoutConfig(
    val validity: Duration,
    val timeSource: () -> Long = System::currentTimeMillis,
) {
    val currentExpiryTime: Long
        get() = timeSource() + validity.toMillis()

    fun isPassed(timeMillis: Long): Boolean = timeMillis < timeSource()
}

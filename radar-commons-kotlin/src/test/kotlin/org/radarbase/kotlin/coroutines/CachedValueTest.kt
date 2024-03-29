package org.radarbase.kotlin.coroutines

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.`is`
import org.hamcrest.Matchers.lessThan
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

@OptIn(DelicateCoroutinesApi::class)
internal class CachedValueTest {
    private lateinit var config: CacheConfig

    private val calls: AtomicInteger = AtomicInteger(0)

    @BeforeEach
    fun setUp() {
        calls.set(0)
        config = CacheConfig(
            refreshDuration = 40.milliseconds,
            retryDuration = 20.milliseconds,
            exceptionCacheDuration = 20.milliseconds,
        )
    }

    @Test
    fun get() {
        val cache = CachedValue(config) { calls.incrementAndGet() }
        runBlocking(GlobalScope.coroutineContext) {
            assertThat("Initial value should refresh", cache.get(), `is`(1))
            assertThat("No refresh within threshold", cache.get(), `is`(1))
            delay(20.milliseconds)
            assertThat("Refresh after threshold", cache.get(), `is`(2))
            assertThat("No refresh after threshold", cache.get(), `is`(2))
        }
    }

    @Test
    fun getInvalid() {
        val cache = CachedValue(config) { calls.incrementAndGet() }
        runBlocking {
            assertThat("Initial value should refresh", cache.get { it < 0 }, equalTo(CachedValue.CacheMiss(1)))
            assertThat("No refresh within threshold", cache.get { it < 0 }, equalTo(CachedValue.CacheHit(1)))
            delay(20.milliseconds)
            assertThat("Refresh after threshold", cache.get { it < 0 }, equalTo(CachedValue.CacheMiss(2)))
            assertThat("No refresh after threshold", cache.get { it < 0 }, equalTo(CachedValue.CacheHit(2)))
        }
    }

    @Test
    fun getValid() {
        val cache = CachedValue(config) { calls.incrementAndGet() }
        runBlocking {
            assertThat("Initial value should refresh", cache.get { it >= 0 }, equalTo(CachedValue.CacheMiss(1)))
            assertThat("No refresh within threshold", cache.get { it >= 0 }, equalTo(CachedValue.CacheHit(1)))
            delay(20.milliseconds)
            assertThat("No refresh after valid value", cache.get { it >= 0 }, equalTo(CachedValue.CacheHit(1)))
        }
    }

    @Test
    fun refresh() {
        val cache = CachedValue(config) { calls.incrementAndGet() }

        runBlocking {
            assertThat("Initial get calls supplier", cache.get(), `is`(1))
            assertThat("Next get uses cache", cache.get(), `is`(1))
            cache.clear()
            assertThat("Next get uses cache", cache.get(), `is`(2))
        }
    }

    @Test
    fun query() {
        val cache = CachedValue(config) { calls.incrementAndGet() }

        runBlocking {
            assertThat("Initial value should refresh", cache.query({ it + 1 }, { it > 2 }), equalTo(CachedValue.CacheMiss(2)))
            assertThat("No refresh within threshold", cache.query({ it + 1 }, { it > 2 }), equalTo(CachedValue.CacheHit(2)))
            delay(20.milliseconds)
            assertThat(
                "Retry because predicate does not match",
                cache.query({ it + 1 }, { it > 2 }),
                equalTo(CachedValue.CacheMiss(3)),
            )
            assertThat("No refresh within threshold", cache.query({ it + 1 }, { it > 2 }), equalTo(CachedValue.CacheHit(3)))
            delay(20.milliseconds)
            assertThat(
                "No retry because predicate matches",
                cache.query({ it + 1 }, { it > 2 }),
                equalTo(CachedValue.CacheHit(3)),
            )
            delay(20.milliseconds)
            assertThat(
                "Refresh after refresh threshold since last retry",
                cache.query({ it + 1 }, { it > 2 }),
                equalTo(CachedValue.CacheMiss(4)),
            )
        }
    }

    @Test
    fun getMultithreaded() {
        val cache = CachedValue(config) {
            calls.incrementAndGet()
            delay(100.milliseconds)
            calls.get()
        }

        runBlocking {
            (0..5)
                .forkJoin {
                    cache.get()
                }
                .forEach {
                    assertThat("Get the same value in all contexts", it, `is`(1))
                }
        }

        assertThat("No more calls are made", calls.get(), `is`(1))
    }

    @Test
    fun getMulti2threaded() {
        val cache = CachedValue(
            config.copy(
                maxSimultaneousCompute = 2,
            ),
        ) {
            calls.incrementAndGet()
            delay(100.milliseconds)
            calls.get()
        }

        runBlocking {
            val values = (0..5)
                .forkJoin {
                    cache.get()
                }

            assertThat(values[0], lessThan(3))
            values.forEach {
                assertThat("Get the same value in all contexts", it, `is`(values[0]))
            }
        }

        assertThat("Two threads should be computing the value", calls.get(), `is`(2))
    }

    @Test
    fun throwTest() {
        val cache = CachedValue(config.copy(refreshDuration = 40.milliseconds)) {
            val newValue = calls.incrementAndGet()
            if (newValue % 2 == 0) throw IllegalStateException() else newValue
        }

        runBlocking {
            assertThat(cache.get(), `is`(1))
            assertThat(cache.get(), `is`(1))
            delay(42.milliseconds)
            assertThrows<IllegalStateException> { cache.get() }
            assertThrows<Exception> { cache.get() }
            delay(22.milliseconds)
            assertThat(cache.get(), `is`(3))
        }
    }
}

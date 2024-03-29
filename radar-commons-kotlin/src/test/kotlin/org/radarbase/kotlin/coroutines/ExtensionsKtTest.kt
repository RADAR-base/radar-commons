package org.radarbase.kotlin.coroutines

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.greaterThan
import org.hamcrest.Matchers.lessThan
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.measureTime

class ExtensionsKtTest {
    companion object {
        @BeforeAll
        @JvmStatic
        fun setUpClass() {
            runBlocking {
                println("warmed up coroutines")
            }
        }
    }

    @Test
    fun testConsumeFirst() = runBlocking {
        val inBlockingTime = measureTime {
            val first = consumeFirst { emit ->
                listOf(
                    async(Dispatchers.Default) {
                        delay(200.milliseconds)
                        emit("a")
                        fail("Should be cancelled")
                    },
                    async(Dispatchers.Default) {
                        delay(50.milliseconds)
                        emit("b")
                    },
                ).awaitAll()
            }
            assertEquals("b", first)
        }
        assertThat(inBlockingTime, greaterThan(50.milliseconds))
        assertThat(inBlockingTime, lessThan(200.milliseconds))
    }

    @Test
    fun testForkJoin() = runBlocking {
        val inBlockingTime = measureTime {
            val result = listOf(100.milliseconds, 50.milliseconds)
                .forkJoin {
                    delay(it)
                    it
                }
            assertEquals(listOf(100.milliseconds, 50.milliseconds), result)
        }
        assertThat(inBlockingTime, greaterThan(100.milliseconds))
    }

    @Test
    fun testForkJoinFirst() = runBlocking {
        val inBlockingTime = measureTime {
            val result: Duration? = consumeFirst { emit ->
                listOf(200.milliseconds, 50.milliseconds)
                    .forkJoin {
                        delay(it)
                        emit(it)
                    }
                emit(null)
            }
            assertEquals(50.milliseconds, result)
        }
        assertThat(inBlockingTime, lessThan(200.milliseconds))
        assertThat(inBlockingTime, greaterThan(50.milliseconds))
    }

    @Test
    fun testConcurrentAny() {
        runBlocking {
            assertTrue(listOf(1, 2, 3, 4).forkAny { it > 3 })
            assertFalse(listOf(1, 2, 3, 4).forkAny { it < 1 })
        }
    }

    @Test
    fun testDoubleCancel() {
        val job = SupervisorJob()
        runBlocking {
            job.invokeOnCompletion {
                println("End")
            }
            job.cancelAndJoin()
            job.invokeOnCompletion {
                println("End")
            }
            job.cancelAndJoin()
        }
    }
}

package org.radarbase.kotlin.coroutines.mutex

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.radarbase.appserver.jersey.utils.withReentrantLock
import java.util.concurrent.atomic.AtomicInteger

class ReentrantMutexTest {

    @Test
    fun testNestedReentrantLock() {
        runBlocking {
            val mutex = Mutex()
            val counter = AtomicInteger(0)

            mutex.withReentrantLock {
                counter.incrementAndGet()
                mutex.withReentrantLock {
                    counter.incrementAndGet()
                }
            }

            assertEquals(2, counter.get(), "Both outer and inner blocks should run")
        }
    }

    @Test
    fun testMutualExclusionAcrossCoroutines() = runBlocking {
        val mutex = Mutex()
        val results = mutableListOf<Int>()

        val job1 = launch {
            mutex.withReentrantLock {
                results += 3
                delay(50)
                results += 4
            }
        }

        val job2 = launch {
            delay(10)
            mutex.withReentrantLock {
                results += 1
                results += 2
            }
        }

        joinAll(job1, job2)

        // Because of the lock, job2’s entries must come after job1 completes:
        assertEquals(listOf(3, 4, 1, 2), results)
    }
}

package org.radarbase.util

import org.junit.Assert
import org.junit.Test
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import kotlin.text.Charsets.UTF_8

class Base64Test {
    @Test
    fun encoderTest() {
        val javaEncoder = Base64.getEncoder()
        val random = ThreadLocalRandom.current()
        var i = 0
        while (i < 2000) {
            val src = ByteArray(i)
            random.nextBytes(src)
            Assert.assertEquals(
                Base64Encoder.encode(src),
                String(javaEncoder.encode(src), UTF_8),
            )
            i += 7
        }
    }
}

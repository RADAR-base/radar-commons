package org.radarbase.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Base64
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
            assertEquals(
                Base64Encoder.encode(src),
                String(javaEncoder.encode(src), UTF_8),
            )
            i += 7
        }
    }
}

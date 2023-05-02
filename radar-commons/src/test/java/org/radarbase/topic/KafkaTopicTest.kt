package org.radarbase.topic

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.*

class KafkaTopicTest {
    @Test
    fun invalidTopicName() {
        assertThrows<IllegalArgumentException> {
            KafkaTopic("bla$")
        }
    }

    @Test
    fun testName() {
        val topic = KafkaTopic("aba")
        assertEquals("aba", topic.name)
    }

    @Test
    @Throws(Exception::class)
    fun compare() {
        val randomSize = 100
        val randomString: MutableList<String> = ArrayList(randomSize)
        val randomTopic: MutableList<KafkaTopic> = ArrayList(randomSize)
        for (i in 0 until randomSize) {
            val str = 'a'.toString() + UUID.randomUUID().toString().replace('-', '_')
            randomString.add(str)
            randomTopic.add(KafkaTopic(str))
        }
        randomString.sort()
        randomTopic.sort()
        for (i in 0 until randomSize) {
            assertEquals(randomString[i], randomTopic[i].name)
        }
    }
}

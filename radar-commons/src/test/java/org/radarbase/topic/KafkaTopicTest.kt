package org.radarbase.topic

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

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
        val randomString: MutableList<String> = (0 until randomSize)
            .mapTo(ArrayList(randomSize)) { 'a'.toString() + UUID.randomUUID().toString().replace('-', '_') }
        val randomTopic: MutableList<KafkaTopic> = randomString
            .mapTo(ArrayList(randomSize)) { KafkaTopic(it) }

        randomString.shuffle()
        randomTopic.shuffle()
        randomString.sort()
        randomTopic.sort()

        randomString.zip(randomTopic)
            .forEach { (s, t) ->
                assertEquals(s, t.name)
            }
    }
}

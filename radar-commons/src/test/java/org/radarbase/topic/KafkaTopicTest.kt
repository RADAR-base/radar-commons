package org.radarbase.topic

import org.junit.Assert
import org.junit.Test
import java.util.*

class KafkaTopicTest {
    @Test(expected = IllegalArgumentException::class)
    fun invalidTopicName() {
        KafkaTopic("bla$")
    }

    @Test
    fun testName() {
        val topic = KafkaTopic("aba")
        Assert.assertEquals("aba", topic.name)
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
            Assert.assertEquals(randomString[i], randomTopic[i].name)
        }
    }
}

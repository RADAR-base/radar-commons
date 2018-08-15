package org.radarcns.topic;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class KafkaTopicTest {
    @Test(expected = IllegalArgumentException.class)
    public void nullArguments() {
        new KafkaTopic(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidTopicName() {
        new KafkaTopic("bla$");
    }


    @Test
    public void getName() {
        KafkaTopic topic = new KafkaTopic("aba");
        assertEquals("aba", topic.getName());
    }


    @Test
    public void compare() throws Exception {
        final int randomSize = 100;
        List<String> randomString = new ArrayList<>(randomSize);
        List<KafkaTopic> randomTopic = new ArrayList<>(randomSize);
        for (int i = 0; i < randomSize; i++) {
            String str = 'a' + UUID.randomUUID().toString().replace('-', '_');
            randomString.add(str);
            randomTopic.add(new KafkaTopic(str));
        }

        Collections.sort(randomString);
        Collections.sort(randomTopic);

        for (int i = 0; i < randomSize; i++) {
            assertEquals(randomString.get(i), randomTopic.get(i).getName());
        }
    }
}

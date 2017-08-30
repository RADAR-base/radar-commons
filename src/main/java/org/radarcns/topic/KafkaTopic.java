/*
 * Copyright 2017 The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.topic;

import java.util.regex.Pattern;

/**
 * A topic that used by Apache Kafka.
 */
public class KafkaTopic {
    private final String name;
    private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]*");

    /**
     * Kafka topic with given name.
     * @param name topic name inside the Kafka cluster
     * @throws IllegalArgumentException if the topic name is null or is not ASCII-alphanumeric with
     *     possible underscores.
     */
    public KafkaTopic(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Kafka topic name may not be null");
        }
        if (!TOPIC_NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Kafka topic " + name + " is not ASCII-alphanumeric "
                    + "with possible underscores.");
        }
        this.name = name;
    }

    /**
     * Get the topic name.
     * @return topic name
     */
    public String getName() {
        return this.name;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KafkaTopic topic = (KafkaTopic) o;

        return name.equals(topic.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "<" + name + ">";
    }
}

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
package org.radarbase.topic

/**
 * A topic that used by Apache Kafka.
 *
 * @param name topic name inside the Kafka cluster
 * @throws IllegalArgumentException if the topic name is null or is not ASCII-alphanumeric with
 * possible underscores.
*/
open class KafkaTopic(
    val name: String
) : Comparable<KafkaTopic> {
    init {
        require(name.matches(TOPIC_NAME_PATTERN)) {
            ("Kafka topic " + name + " is not ASCII-alphanumeric "
                    + "with possible underscores.")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        other as KafkaTopic
        return name == other.name
    }

    override fun hashCode(): Int = name.hashCode()

    override fun toString(): String = javaClass.simpleName + "<" + name + ">"

    override fun compareTo(other: KafkaTopic): Int = name.compareTo(other.name)

    companion object {
        private val TOPIC_NAME_PATTERN = "[a-zA-Z][a-zA-Z0-9_]*".toRegex()
    }
}

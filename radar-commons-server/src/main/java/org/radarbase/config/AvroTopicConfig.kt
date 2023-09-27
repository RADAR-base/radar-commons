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
package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.avro.specific.SpecificRecord
import org.radarbase.topic.AvroTopic
import org.radarbase.topic.AvroTopic.Companion.parse

/**
 * Specifies an Avro topic.
 */
@OpenConfig
class AvroTopicConfig {
    var topic: String? = null

    @JsonProperty("key_schema")
    var keySchema: String? = null

    @JsonProperty("value_schema")
    var valueSchema: String? = null
    var tags: List<String>? = null

    /**
     * Parse an AvroTopic from the values in this class.
     *
     * @throws IllegalStateException if the key_schema or value_schema properties are not valid
     * Avro SpecificRecord classes
     */
    fun <K : SpecificRecord, V : SpecificRecord> parseAvroTopic(): AvroTopic<K, V> {
        return try {
            parse(
                checkNotNull(topic) { "Topic is not specified" },
                checkNotNull(keySchema) { "Key schema is not specified" },
                checkNotNull(valueSchema) { "Value schema is not specified" },
            )
        } catch (ex: IllegalArgumentException) {
            throw IllegalStateException(
                "Topic $topic schema cannot be instantiated",
                ex,
            )
        }
    }
}

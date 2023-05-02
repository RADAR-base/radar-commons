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

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

/**
 * AvroTopic used by sensors. This has additional verification on the schemas that are used compared
 * to AvroTopic.
 *
 * @param name topic name
 * @param keySchema key schema
 * @param valueSchema value schema
 * @param keyClass actual key class
 * @param valueClass actual value class
*/
class SensorTopic<K : Any, V : Any>(
    name: String,
    keySchema: Schema,
    valueSchema: Schema,
    keyClass: Class<out K>,
    valueClass: Class<out V>,
) : AvroTopic<K, V>(name, keySchema, valueSchema, keyClass, valueClass) {

    init {
        require(keySchema.type == Schema.Type.RECORD) { "Sensors must send records as keys" }
        require(valueSchema.type == Schema.Type.RECORD) { "Sensors must send records as values" }
        requireNotNull(keySchema.getField("projectId")) { "Key schema must have a project ID" }
        requireNotNull(keySchema.getField("userId")) { "Key schema must have a user ID" }
        requireNotNull(keySchema.getField("sourceId")) { "Key schema must have a source ID" }
        requireNotNull(valueSchema.getField("time")) { "Schema must have time as its first field" }
        requireNotNull(valueSchema.getField("timeReceived")) { "Schema must have timeReceived as a field" }
    }

    companion object {
        /**
         * Parse a SensorTopic.
         *
         * @throws IllegalArgumentException if the key_schema or value_schema properties are not valid
         * Avro SpecificRecord classes
         */
        @JvmStatic
        inline fun <reified K : SpecificRecord, reified V : SpecificRecord> parse(
            topic: String,
            keySchema: String,
            valueSchema: String,
        ): SensorTopic<K, V> {
            val parseAvro = AvroTopic.parse<K, V>(topic, keySchema, valueSchema)
            return SensorTopic(
                parseAvro.name,
                parseAvro.keySchema,
                parseAvro.valueSchema,
                parseAvro.keyClass,
                parseAvro.valueClass,
            )
        }
    }
}

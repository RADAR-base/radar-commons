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
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificRecord
import java.util.*

/**
 * Kafka topic with Avro schema.
 * @param name topic name
 * @param keySchema Avro schema for keys
 * @param valueSchema Avro schema for values
 * @param keyClass Java class for keys
 * @param valueClass Java class for values
 */
open class AvroTopic<K: Any, V: Any>(
    name: String,
    val keySchema: Schema,
    val valueSchema: Schema,
    val keyClass: Class<out K>,
    val valueClass: Class<out V>,
) : KafkaTopic(name) {
    val valueFieldTypes: Array<Schema.Type>? = if (valueSchema.type == Schema.Type.RECORD) {
        val fields = valueSchema.fields
        Array(fields.size) { i ->
            fields[i].schema().type
        }
    } else null
        get() = field?.copyOf()

    /**
     * Tries to construct a new SpecificData instance of the value.
     * @return new empty SpecificData class
     * @throws ClassCastException Value class is not a SpecificData class
     */
    @Suppress("UNCHECKED_CAST")
    @Throws(ClassCastException::class)
    fun newValueInstance(): V {
        return SpecificData.newInstance(valueClass, valueSchema) as V
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (!super.equals(other)) {
            return false
        }
        other as AvroTopic<*, *>
        return keyClass == other.keyClass && valueClass == other.valueClass
    }

    override fun hashCode(): Int = name.hashCode()

    companion object {
        /**
         * Parse an AvroTopic.
         *
         * @throws IllegalArgumentException if the key_schema or value_schema properties are not valid
         * Avro SpecificRecord classes
         */
        fun <K : SpecificRecord, V : SpecificRecord> parse(
            topic: String,
            keySchema: String,
            valueSchema: String,
        ): AvroTopic<K, V> {
            val key = parseSpecificRecord<K>(keySchema)
            val value = parseSpecificRecord<V>(valueSchema)
            return AvroTopic(
                topic,
                key.schema,
                value.schema,
                key.javaClass,
                value.javaClass,
            )
        }

        /**
         * Parse the schema of a single specific record.
         *
         * @param schemaClass class name of the SpecificRecord to use
         * @param <K> class type to return
         * @return Instantiated class of given specific record class
        </K> */
        @Suppress("UNCHECKED_CAST")
        fun <K : SpecificRecord> parseSpecificRecord(schemaClass: String): K {
            return try {
                val keyClass = Class.forName(schemaClass)
                val keyAvroSchema = keyClass
                    .getMethod("getClassSchema").invoke(null) as Schema
                // check instantiation
                SpecificData.newInstance(keyClass, keyAvroSchema) as K
            } catch (ex: ClassCastException) {
                throw IllegalArgumentException(
                    "Schema $schemaClass cannot be instantiated",
                    ex
                )
            } catch (ex: ReflectiveOperationException) {
                throw IllegalArgumentException(
                    "Schema $schemaClass cannot be instantiated",
                    ex
                )
            }
        }
    }
}

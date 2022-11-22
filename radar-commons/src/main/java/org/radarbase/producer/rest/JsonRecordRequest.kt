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
package org.radarbase.producer.rest

import okio.Buffer
import okio.BufferedSink
import org.apache.avro.SchemaValidationException
import org.radarbase.data.AvroEncoder.AvroWriter
import org.radarbase.data.RecordData
import org.radarbase.data.RemoteSchemaEncoder
import org.radarbase.topic.AvroTopic
import java.io.IOException
import java.nio.charset.StandardCharsets

/**
 * Request data to submit records to the Kafka REST proxy.
 */
class JsonRecordRequest<K: Any, V: Any>(topic: AvroTopic<K, V>) : RecordRequest<K, V> {
    private val keyEncoder: AvroWriter<K>
    private val valueEncoder: AvroWriter<V>
    private var records: RecordData<K, V>? = null

    /**
     * Generate a record request for given topic.
     * @param topic topic to use.
     * @throws IllegalStateException if key or value encoders could not be made.
     */
    init {
        val schemaEncoder = RemoteSchemaEncoder(false)
        keyEncoder = schemaEncoder.writer(topic.keySchema, topic.keyClass)
        valueEncoder = schemaEncoder.writer(topic.valueSchema, topic.valueClass)
    }

    /**
     * Writes the current topic to a stream. This implementation does not use any JSON writers to
     * write the data, but writes it directly to a stream. [JSONObject.quote]
     * is used to get the correct formatting. This makes the method as lean as possible.
     * @param sink buffered sink to write to.
     * @throws IOException if a superimposing stream could not be created
     */
    @Throws(IOException::class)
    override fun writeToSink(sink: BufferedSink) {
        writeToSink(sink, Int.MAX_VALUE)
    }

    @Throws(IOException::class)
    private fun writeToSink(sink: BufferedSink, maxLength: Int) {
        val keySchema = checkNotNull(keyEncoder.readerSchema) {
            "Record request has not been prepared with the proper reader schemas"
        }
        val valueSchema = checkNotNull(valueEncoder.readerSchema) {
            "Record request has not been prepared with the proper reader schemas"
        }

        sink.writeByte('{'.code)
        sink.write(KEY_SCHEMA_ID)
        sink.write(keySchema.id.toString().toByteArray())
        sink.write(VALUE_SCHEMA_ID)
        sink.write(valueSchema.id.toString().toByteArray())
        sink.write(RECORDS)
        val key = keyEncoder.encode(records!!.key)
        var curLength = KEY_SCHEMA_ID.size + VALUE_SCHEMA_ID.size + 7
        var first = true
        for (record in records!!) {
            if (curLength >= maxLength) {
                return
            }
            if (first) {
                first = false
            } else {
                sink.writeByte(','.code)
            }
            sink.write(KEY)
            sink.write(key)
            sink.write(VALUE)
            val valueBytes = valueEncoder.encode(record)
            sink.write(valueBytes)
            sink.writeByte('}'.code)
            curLength += 2 + key.size + KEY.size + VALUE.size + valueBytes.size
        }
        sink.write(END)
    }

    override fun reset() {
        records = null
    }

    @Throws(SchemaValidationException::class)
    override fun prepare(
        keySchema: ParsedSchemaMetadata,
        valueSchema: ParsedSchemaMetadata,
        records: RecordData<K, V>
    ) {
        keyEncoder.readerSchema = keySchema
        valueEncoder.readerSchema = valueSchema
        this.records = records
    }

    @Throws(IOException::class)
    override fun content(maxLength: Int): String {
        Buffer().use { buffer ->
            writeToSink(buffer, maxLength)
            return buffer.readString(
                buffer.size.coerceAtMost(maxLength.toLong()),
                StandardCharsets.UTF_8
            )
        }
    }

    companion object {
        val KEY_SCHEMA_ID = "\"key_schema_id\":".toByteArray()
        val VALUE_SCHEMA_ID = ",\"value_schema_id\":".toByteArray()
        val RECORDS = ",\"records\":[".toByteArray()
        val KEY = "{\"key\":".toByteArray()
        val VALUE = ",\"value\":".toByteArray()
        val END = "]}".toByteArray()
    }
}

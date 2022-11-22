/*
 * Copyright 2018 The Hyve
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
import org.apache.avro.Schema
import org.apache.avro.SchemaValidationException
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import org.radarbase.data.AvroEncoder.AvroWriter
import org.radarbase.data.RecordData
import org.radarbase.data.RemoteSchemaEncoder
import org.radarbase.producer.rest.AvroDataMapperFactory.Companion.validationException
import org.radarbase.topic.AvroTopic
import org.radarbase.util.Strings.toHexString
import java.io.IOException
import java.lang.IllegalArgumentException

/**
 * Encodes a record request as binary data, in the form of a RecordSet.
 * @param <K> record key type
 * @param <V> record value type
</V></K> */
class BinaryRecordRequest<K: Any, V: Any>(topic: AvroTopic<K, V>) : RecordRequest<K, V> {
    private var keyVersion = 0
    private var valueVersion = 0
    private var records: RecordData<K, V>? = null
    private var binaryEncoder: BinaryEncoder? = null
    private val valueEncoder: AvroWriter<V>
    private var sourceIdPos = 0

    /**
     * Binary record request for given topic.
     * @param topic topic to send data for.
     * @throws SchemaValidationException if the key schema does not contain a
     * `sourceId` field.
     * @throws IllegalArgumentException if the topic cannot be used to make a AvroWriter.
     */
    init {
        if (topic.keySchema.type != Schema.Type.RECORD) {
            throw validationException(
                topic.keySchema, topic.keySchema,
                "Cannot use non-record key schema"
            )
        }
        val sourceIdField = topic.keySchema.getField("sourceId")
        sourceIdPos = sourceIdField?.pos()
            ?: throw validationException(
                topic.keySchema, topic.keySchema,
                "Cannot use binary encoder without a source ID."
            )
        valueEncoder = RemoteSchemaEncoder(true)
            .writer(topic.valueSchema, topic.valueClass)
    }

    @Throws(IOException::class)
    override fun writeToSink(sink: BufferedSink) {
        writeToSink(sink, Int.MAX_VALUE)
    }

    @Throws(IOException::class)
    private fun writeToSink(sink: BufferedSink, maxLength: Int) {
        binaryEncoder = EncoderFactory.get().directBinaryEncoder(
            sink.outputStream(), binaryEncoder
        )
        binaryEncoder?.writeRecords(
            records ?: return,
            maxLength
        )
    }

    private fun BinaryEncoder.writeRecords(records: RecordData<K, V>, maxLength: Int) {
        startItem()
        writeInt(keyVersion)
        writeInt(valueVersion)

        // do not send project ID; it is encoded in the serialization
        writeIndex(0)
        // do not send user ID; it is encoded in the serialization
        writeIndex(0)
        val sourceId = (records.key as IndexedRecord)[sourceIdPos].toString()
        writeString(sourceId)
        writeArrayStart()
        setItemCount(records.size().toLong())
        var curLength = 18 + sourceId.length
        for (record in records) {
            if (curLength >= maxLength) {
                return
            }
            startItem()
            val valueBytes = valueEncoder.encode(record)
            writeBytes(valueBytes)
            curLength += 4 + valueBytes.size
        }
        writeArrayEnd()
        flush()
    }

    override fun reset() {
        records = null
    }

    @Throws(SchemaValidationException::class)
    override fun prepare(
        keySchema: ParsedSchemaMetadata, valueSchema: ParsedSchemaMetadata,
        records: RecordData<K, V>
    ) {
        keyVersion = if (keySchema.version == null) 0 else keySchema.version
        valueVersion = if (valueSchema.version == null) 0 else valueSchema.version
        valueEncoder.readerSchema = valueSchema
        this.records = records
    }

    @Throws(IOException::class)
    override fun content(maxLength: Int): String {
        Buffer().use { buffer ->
            writeToSink(buffer, maxLength / 2 - 2)
            val printSize = buffer.size.coerceAtMost((maxLength - 2).toLong())
            return "0x" + buffer.readByteArray(printSize).toHexString()
        }
    }
}

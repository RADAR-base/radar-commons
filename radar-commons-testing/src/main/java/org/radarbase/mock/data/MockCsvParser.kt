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
package org.radarbase.mock.data

import com.opencsv.CSVReader
import com.opencsv.exceptions.CsvValidationException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.specific.SpecificRecord
import org.radarbase.data.Record
import org.radarbase.mock.config.MockDataConfig
import org.radarbase.producer.schema.SchemaRetriever
import org.radarbase.topic.AvroTopic
import org.radarbase.topic.AvroTopic.Companion.parseSpecificRecord
import java.io.BufferedReader
import java.io.Closeable
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.io.path.bufferedReader

/**
 * Parse mock data from a CSV file.
 *
 * @param config configuration of the stream.
 * @param root parent directory of the data file.
 * @param retriever schema retriever to fetch schema with if none is supplied.
 * @throws IllegalArgumentException if the second row has the wrong number of columns
 */
class MockCsvParser constructor(
    private val config: MockDataConfig,
    root: Path?,
    private val startTime: Instant,
    private val retriever: SchemaRetriever,
) : Closeable {
    lateinit var topic: AvroTopic<GenericRecord, GenericRecord>
    private val csvReader: CSVReader
    private val bufferedReader: BufferedReader
    private val rowDuration: Duration = Duration.ofMillis((1.0 / config.frequency).toLong())
    private val headers: HeaderHierarchy
    private var currentLine: Array<String>?
    private var row: Int = 0
    private var rowTime: Long = this.startTime.toEpochMilli()

    init {
        bufferedReader = config.getDataFile(root).bufferedReader()
        csvReader = CSVReader(bufferedReader)
        headers = HeaderHierarchy()
        val header = csvReader.readNext()
        for (i in header.indices) {
            headers.add(
                i,
                header[i].split("\\.".toRegex()).dropLastWhile { it.isEmpty() },
            )
        }
        currentLine = csvReader.readNext()
    }

    suspend fun initialize() {
        val (keySchema, valueSchema) = try {
            val specificTopic = config.parseAvroTopic<SpecificRecord, SpecificRecord>()
            Pair(specificTopic.keySchema, specificTopic.valueSchema)
        } catch (ex: IllegalStateException) {
            Pair(
                parseSpecificRecord<SpecificRecord>(config.keySchema).schema,
                retriever.getByVersion(config.topic, true, 0).schema,
            )
        }

        topic = AvroTopic(
            config.topic,
            keySchema,
            valueSchema,
            GenericRecord::class.java,
            GenericRecord::class.java,
        )
    }

    /**
     * Read the next record in the file.
     * @throws NullPointerException if a field from the Avro schema is missing as a column
     * @throws IllegalArgumentException if the row has the wrong number of columns
     * @throws IllegalStateException if a next row is not available
     * @throws IOException if the next row could not be read
     */
    @Throws(IOException::class, CsvValidationException::class)
    operator fun next(): Record<GenericRecord, GenericRecord> {
        check(hasNext()) { "No next record available" }
        val key = parseRecord(
            currentLine,
            topic.keySchema,
            checkNotNull(headers.children["key"]) { "Missing key fields" },
        )
        val value = parseRecord(
            currentLine,
            topic.valueSchema,
            checkNotNull(headers.children["value"]) { "Missing value fields" },
        )
        incrementRow()
        return Record(key, value)
    }

    @Throws(CsvValidationException::class, IOException::class)
    private fun incrementRow() {
        currentLine = csvReader.readNext()
        row++
        rowTime = startTime
            .plus(rowDuration.multipliedBy(row.toLong()))
            .toEpochMilli()
    }

    /**
     * Whether there is a next record in the file.
     */
    operator fun hasNext(): Boolean {
        return currentLine != null
    }

    private fun parseRecord(
        rawValues: Array<String>?,
        schema: Schema,
        headers: HeaderHierarchy,
    ): GenericRecord {
        val record = GenericRecordBuilder(schema)
        val children = headers.children
        for (field in schema.fields) {
            val child = children[field.name()]
            if (child != null) {
                record[field] = parseValue(rawValues, field.schema(), child)
            }
        }
        return record.build()
    }

    /** Parse value from Schema.  */
    fun parseValue(rawValues: Array<String>?, schema: Schema, headers: HeaderHierarchy): Any? {
        return when (schema.type) {
            Schema.Type.NULL, Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.BOOLEAN, Schema.Type.STRING, Schema.Type.ENUM, Schema.Type.BYTES -> parseScalar(
                rawValues,
                schema,
                headers,
            )

            Schema.Type.UNION -> parseUnion(rawValues, schema, headers)
            Schema.Type.RECORD -> parseRecord(rawValues, schema, headers)
            Schema.Type.ARRAY -> parseArray(rawValues, schema, headers)
            Schema.Type.MAP -> parseMap(rawValues, schema, headers)
            else -> throw IllegalArgumentException(
                "Cannot handle schemas of type " +
                    schema.type + " in " + headers,
            )
        }
    }

    private fun parseScalar(
        rawValues: Array<String>?,
        schema: Schema,
        headers: HeaderHierarchy,
    ): Any? {
        val fieldHeader = headers.index
        require(fieldHeader < rawValues!!.size) { "Row is missing value for " + headers.name }
        val fieldString = rawValues[fieldHeader]
            .replace("\${timeSeconds}", java.lang.Double.toString(rowTime / 1000.0))
            .replace("\${timeMillis}", java.lang.Long.toString(rowTime))
        return parseScalar(fieldString, schema, headers)
    }

    private fun parseMap(
        rawValues: Array<String>?,
        schema: Schema,
        headers: HeaderHierarchy,
    ): Map<String, Any?> = buildMap {
        for (child in headers.children.values) {
            put(child.name!!, parseValue(rawValues, schema.valueType, child))
        }
    }

    private fun parseUnion(
        rawValues: Array<String>?,
        schema: Schema,
        headers: HeaderHierarchy,
    ): Any = requireNotNull(
        schema.types.firstNotNullOfOrNull { subSchema ->
            try {
                parseValue(rawValues, subSchema, headers)
            } catch (ex: IllegalArgumentException) {
                // skip bad union member
                null
            }
        },
    ) { "Cannot handle union types ${schema.types} in $headers" }

    private fun parseArray(
        rawValues: Array<String>?,
        schema: Schema,
        headers: HeaderHierarchy,
    ): List<Any?> {
        val children = headers.children
        val arrayLength = children.keys.stream()
            .mapToInt { headerName: String -> headerName.toInt() + 1 }
            .max()
            .orElse(0)
        val array = GenericData.Array<Any?>(arrayLength, schema)
        for (i in 0 until arrayLength) {
            val child = children[i.toString()]
            if (child != null) {
                array.add(i, parseValue(rawValues, schema.elementType, child))
            } else {
                array.add(i, null)
            }
        }
        return array
    }

    @Throws(IOException::class)
    override fun close() {
        csvReader.close()
        bufferedReader.close()
    }

    override fun toString(): String {
        return "MockCsvParser{topic=$topic}"
    }

    companion object {
        private fun parseScalar(
            fieldString: String?,
            schema: Schema,
            headers: HeaderHierarchy,
        ): Any? {
            return when (schema.type) {
                Schema.Type.NULL -> if (fieldString.isNullOrEmpty() || fieldString == "null") {
                    null
                } else {
                    throw IllegalArgumentException("Cannot parse $fieldString as null")
                }
                Schema.Type.INT -> fieldString!!.toInt()
                Schema.Type.LONG -> fieldString!!.toLong()
                Schema.Type.FLOAT -> fieldString!!.toFloat()
                Schema.Type.DOUBLE -> fieldString!!.toDouble()
                Schema.Type.BOOLEAN -> java.lang.Boolean.parseBoolean(fieldString)
                Schema.Type.STRING -> fieldString
                Schema.Type.ENUM -> parseEnum(schema, fieldString)
                Schema.Type.BYTES -> parseBytes(fieldString)
                else -> throw IllegalArgumentException(
                    "Cannot handle scalar schema of type " +
                        schema.type + " in " + headers,
                )
            }
        }

        private fun parseBytes(fieldString: String?): ByteBuffer {
            val result = Base64.getDecoder()
                .decode(fieldString!!.toByteArray(StandardCharsets.UTF_8))
            return ByteBuffer.wrap(result)
        }

        private fun parseEnum(schema: Schema, fieldString: String?): GenericData.EnumSymbol {
            return GenericData.EnumSymbol(schema, fieldString)
        }
    }
}

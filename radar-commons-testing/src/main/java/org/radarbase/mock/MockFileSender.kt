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
package org.radarbase.mock

import com.opencsv.exceptions.CsvValidationException
import org.apache.avro.SchemaValidationException
import org.apache.avro.generic.GenericRecord
import org.radarbase.data.Record
import org.radarbase.mock.data.MockCsvParser
import org.radarbase.producer.KafkaSender
import java.io.IOException

/**
 * Send mock data from a CSV file.
 *
 *
 * The value type is dynamic, so we will not check any of the generics.
 */
class MockFileSender(
    private val sender: KafkaSender,
    private val parser: MockCsvParser,
) {
    /**
     * Send data from the configured CSV file synchronously.
     * @throws IOException if data could not be read or sent.
     */
    @Throws(IOException::class)
    suspend fun send() {
        parser.initialize()
        try {
            val topicSender = sender.sender(parser.topic)
            while (parser.hasNext()) {
                val record: Record<*, *> = parser.next()
                topicSender.send(record.key as GenericRecord, record.value as GenericRecord)
            }
        } catch (e: SchemaValidationException) {
            throw IOException("Failed to match schemas", e)
        } catch (e: CsvValidationException) {
            throw IOException("Failed to read CSV file", e)
        }
    }

    override fun toString(): String {
        return ("MockFileSender{"
                + "parser=" + parser
                + '}')
    }
}

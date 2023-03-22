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
package org.radarbase.data

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.radarbase.data.AvroDecoder.AvroReader
import java.io.ByteArrayInputStream
import java.io.IOException

/** An AvroDecoder to decode known SpecificRecord classes.  */
/**
 * Decoder for Avro data.
 * @param genericData instance of GenericData or SpecificData that should implement
 * [GenericData.createDatumReader].
 * @param binary true if the read data has Avro binary encoding, false if it has Avro JSON
 * encoding.
 */
class AvroDatumDecoder(
    private val genericData: GenericData,
    private val binary: Boolean,
) : AvroDecoder {
    private val decoderFactory: DecoderFactory = DecoderFactory.get()

    @Suppress("UNCHECKED_CAST")
    override fun <T> reader(schema: Schema, clazz: Class<out T>): AvroReader<T> {
        val reader = genericData.createDatumReader(schema) as DatumReader<T>
        return AvroRecordReader(schema, reader)
    }

    private inner class AvroRecordReader<T>(
        private val schema: Schema,
        private val reader: DatumReader<T>,
    ) : AvroReader<T> {
        private var decoder: Decoder? = null

        @Throws(IOException::class)
        override fun decode(`object`: ByteArray): T {
            return decode(`object`, 0)
        }

        @Throws(IOException::class)
        override fun decode(`object`: ByteArray, offset: Int): T {
            decoder = if (binary) {
                decoderFactory.binaryDecoder(
                    `object`,
                    offset,
                    `object`.size - offset,
                    decoder as? BinaryDecoder,
                )
            } else {
                decoderFactory.jsonDecoder(
                    schema,
                    ByteArrayInputStream(`object`, offset, `object`.size - offset),
                )
            }
            return reader.read(null, decoder)
        }
    }
}

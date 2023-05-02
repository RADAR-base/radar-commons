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
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory
import org.radarbase.data.AvroEncoder.AvroWriter
import java.io.ByteArrayOutputStream
import java.io.IOException

/**
 * Encodes Avro records to bytes.
 *
 * @param encoderFactory encoder factory to use.
 * @param schema schema to write records with.
 * @param writer data writer
 * @param binary true if the data should be serialized with binary Avro encoding, false if it
 * should be with JSON encoding.
 * @throws IOException if an encoder cannot be constructed.
 */
class AvroRecordWriter<T : Any>(
    encoderFactory: EncoderFactory,
    schema: Schema,
    private val writer: DatumWriter<T>,
    binary: Boolean,
) : AvroWriter<T> {
    private val out: ByteArrayOutputStream = ByteArrayOutputStream()
    private var encoder: Encoder = if (binary) {
        encoderFactory.binaryEncoder(out, null)
    } else {
        encoderFactory.jsonEncoder(schema, out)
    }

    init {
        encoder = if (binary) {
            encoderFactory.binaryEncoder(out, null)
        } else {
            encoderFactory.jsonEncoder(schema, out)
        }
    }

    @Throws(IOException::class)
    override fun encode(`object`: T): ByteArray {
        return try {
            writer.write(`object`, encoder)
            encoder.flush()
            out.toByteArray()
        } finally {
            out.reset()
        }
    }
}

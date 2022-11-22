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
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.radarbase.data.AvroEncoder.AvroWriter
import java.io.IOException

/**
 *  An AvroEncoder to encode known SpecificRecord classes.
 *  @param binary whether to use binary encoding or JSON.
 */
class AvroDatumEncoder(
    private val genericData: GenericData,
    private val binary: Boolean,
) : AvroEncoder {
    private val encoderFactory: EncoderFactory = EncoderFactory.get()

    @Throws(IOException::class)
    override fun <T: Any> writer(schema: Schema, clazz: Class<out T>): AvroWriter<T> {
        val writer = genericData.createDatumWriter(schema) as DatumWriter<T>
        return AvroRecordWriter(encoderFactory, schema, writer, binary)
    }
}

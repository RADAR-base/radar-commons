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
package org.radarbase.producer.schema

import org.apache.avro.Schema

/**
 * Parsed schema metadata from a Schema Registry.
 * @param id schema ID, may be null.
 * @param version schema version, may be null.
 * @param schema parsed schema.
 */
data class ParsedSchemaMetadata(
    val id: Int,
    val version: Int?,
    val schema: Schema,
)

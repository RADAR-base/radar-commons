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

import io.ktor.http.HttpStatusCode
import java.io.IOException

/**
 * Exception when a HTTP REST request fails.
 * @param status HTTP status code
 * @param body response body.
 */
class RestException(
    val status: HttpStatusCode,
    body: String? = null,
    cause: Throwable? = null,
) : IOException(
    buildString(150) {
        append("REST call failed (HTTP code ")
        append(status)
        if (body == null) {
            append(')')
        } else {
            append("): ")
            append(
                if (body.length <= 512) {
                    body
                } else {
                    body.substring(0, 512)
                },
            )
        }
    },
    cause,
)

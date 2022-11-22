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

import okhttp3.MediaType
import okhttp3.Request
import okhttp3.RequestBody
import okio.BufferedSink
import java.io.IOException

/**
 * TopicRequestData in a RequestBody.
 */
internal class TopicRequestBody(
    protected val data: RecordRequest<*, *>,
    private val mediaType: MediaType
) : RequestBody() {
    override fun contentType(): MediaType? {
        return mediaType
    }

    @Throws(IOException::class)
    override fun writeTo(sink: BufferedSink) {
        data.writeToSink(sink)
    }

    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun topicRequestContent(request: Request?, maxLength: Int): String? {
            request ?: return null
            val body = request.body as TopicRequestBody? ?: return null
            return body.data.content(maxLength)
        }
    }
}

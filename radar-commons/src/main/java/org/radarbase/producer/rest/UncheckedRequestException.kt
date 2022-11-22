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

import okhttp3.Request
import okhttp3.Response
import org.radarbase.producer.rest.RestClient.Companion.bodyString
import org.radarbase.producer.rest.TopicRequestBody.Companion.topicRequestContent
import java.io.IOException

/** Unchecked exception for failures during request handling.  */
class UncheckedRequestException
/**
 * Unchecked exception.
 * @param message exception message.
 * @param cause cause of this exception, may be null
 */
    (message: String?, cause: IOException?) : RuntimeException(message, cause) {
    /**
     * Rethrow this exception using either its cause, if that is an IOException, or using
     * the current exception.
     * @throws IOException if the cause of the exception was an IOException.
     * @throws UncheckedRequestException if the cause of the exception was not an IOException.
     */
    @Throws(IOException::class)
    fun rethrow() {
        if (cause is IOException) {
            throw (cause as IOException?)!!
        } else {
            throw IOException(this)
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private const val LOG_CONTENT_LENGTH = 1024

        /**
         * Create a new UncheckedRequestException based on given call.
         *
         * @param request call request
         * @param response call response, may be null
         * @param cause exception cause, may be null
         * @return new exception
         * @throws IOException if the request or response cannot be constructed into a message.
         */
        @Throws(IOException::class)
        fun fail(
            request: Request?,
            response: Response?, cause: IOException?
        ): UncheckedRequestException {
            val message = buildString(128) {
                append("FAILED to transmit message")
                val content: String? = if (response != null) {
                    append(" (HTTP status code ")
                    append(response.code)
                    append(')')
                    response.bodyString()
                } else {
                    null
                }
                val requestContent = topicRequestContent(request, LOG_CONTENT_LENGTH)
                if (requestContent != null || content != null) {
                    append(':')
                }
                if (requestContent != null) {
                    append("\n    ")
                    append(requestContent)
                }
                if (content != null) {
                    append("\n    ")
                    append(content)
                }
            }
            return UncheckedRequestException(message, cause)
        }
    }
}

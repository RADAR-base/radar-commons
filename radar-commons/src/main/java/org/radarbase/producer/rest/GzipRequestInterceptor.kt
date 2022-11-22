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

import okhttp3.*
import okio.BufferedSink
import okio.GzipSink
import okio.buffer
import java.io.IOException

/** This interceptor compresses the HTTP request body. Many webservers can't handle this!  */
class GzipRequestInterceptor : Interceptor {
    @Throws(IOException::class)
    override fun intercept(chain: Interceptor.Chain): Response {
        val originalRequest: Request = chain.request()
        if (originalRequest.body == null || originalRequest.header("Content-Encoding") != null) {
            return chain.proceed(originalRequest)
        }

        return chain.proceed(
            originalRequest.newBuilder()
                .header("Content-Encoding", "gzip")
                .method(originalRequest.method, gzip(originalRequest.body))
                .build()
        )
    }

    private fun gzip(body: RequestBody?): RequestBody {
        return object : RequestBody() {
            override fun contentType(): MediaType? = body?.contentType()

            override fun contentLength(): Long {
                return -1 // We don't know the compressed length in advance!
            }

            @Throws(IOException::class)
            override fun writeTo(sink: BufferedSink) {
                GzipSink(sink).buffer().use { gzipSink -> body?.writeTo(gzipSink) }
            }
        }
    }

    override fun hashCode(): Int = 1

    override fun equals(other: Any?): Boolean {
        return this === other || other != null && javaClass == other.javaClass
    }
}

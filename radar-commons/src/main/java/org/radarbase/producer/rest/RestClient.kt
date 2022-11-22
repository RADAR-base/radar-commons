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

import okhttp3.*
import okhttp3.Headers.Companion.headersOf
import org.radarbase.config.ServerConfig
import org.radarbase.util.RestUtils
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.ref.WeakReference
import java.net.MalformedURLException
import java.util.*
import java.util.concurrent.TimeUnit
import javax.net.ssl.X509TrustManager

/** REST client using OkHttp3. This class is not thread-safe.  */
class RestClient private constructor(builder: Builder) {
    /** Configured server.  */
    val server: ServerConfig = requireNotNull(builder.server) { "Missing server configuration" }
    /** OkHttp client.  */
    val httpClient: OkHttpClient = builder.httpClientBuilder.build()
    private val headers: Headers = builder.headers
    private val relativeUrl = server.httpUrl

    /** Configured connection timeout in seconds.  */
    val timeout: Long
        get() = (httpClient.connectTimeoutMillis / 1000).toLong()

    /**
     * Make a blocking request.
     * @param request request, possibly built with [.requestBuilder]
     * @return response to the request
     * @throws IOException if the request fails
     * @throws NullPointerException if the request is null
     */
    @Throws(IOException::class)
    fun request(request: Request): Response {
        return httpClient.newCall(request).execute()
    }

    /**
     * Make a blocking request.
     * @param request request, possibly built with [.requestBuilder]
     * @return response to the request
     * @throws IOException if the request fails
     * @throws NullPointerException if the request is null
     */
    @Throws(IOException::class)
    fun request(builder: Request.Builder.() -> Unit): Response = httpClient.newCall(
        Request.Builder().apply {
            headers(headers)
            builder()
        }.build()
    ).execute()

    /**
     * Make a blocking request.
     * @param request request, possibly built with [.requestBuilder]
     * @return response to the request
     * @throws IOException if the request fails
     * @throws NullPointerException if the request is null
     */
    @Throws(IOException::class)
    inline fun request(
        relativePath: String,
        crossinline builder: Request.Builder.() -> Unit,
    ): Response = request {
        url(relativeUrl(relativePath))
        builder()
    }

    /**
     * Make an asynchronous request.
     * @param request request, possibly built with [.requestBuilder]
     * @param callback callback to activate once the request is done.
     */
    fun request(request: Request, callback: Callback) =
        httpClient.newCall(request).enqueue(callback)

    /**
     * Make a request to given relative path. This does not set any request properties except the
     * URL.
     * @param relativePath relative path to request
     * @return response to the request
     * @throws IOException if the path is invalid or the request failed.
     */
    @Throws(IOException::class)
    fun request(relativePath: String): Response = request(buildRequest(relativePath))

    /**
     * Make a blocking request and return the body.
     * @param request request to make.
     * @return response body string.
     * @throws RestException if no body was returned or an HTTP status code indicating error was
     * returned.
     * @throws IOException if the request cannot be completed or the response cannot be read.
     */
    @Throws(IOException::class)
    fun requestString(request: Request): String {
        request(request).use { response ->
            val bodyString = response.bodyString()
            if (!response.isSuccessful || bodyString == null) {
                throw RestException(response.code, bodyString)
            }
            return bodyString
        }
    }

    /**
     * Create a OkHttp3 request builder with [Request.Builder.url] set.
     * Call[Request.Builder.build] to make the actual request with
     * [.request].
     *
     * @param relativePath relative path from the server serverConfig
     * @return request builder.
     * @throws MalformedURLException if the path not valid
     */
    @Throws(MalformedURLException::class)
    fun requestBuilder(relativePath: String): Request.Builder = Request.Builder()
        .url(relativeUrl(relativePath))
        .headers(headers)

    fun buildRequest(
        relativePath: String,
        builder: Request.Builder.() -> Unit = {},
    ): Request = Request.Builder().apply {
        url(relativeUrl(relativePath))
        headers(headers)
        builder()
    }.build()

    /**
     * Get a URL relative to the configured server.
     * @param path relative path
     * @return URL
     * @throws MalformedURLException if the path is malformed
     */
    @Throws(MalformedURLException::class)
    fun relativeUrl(path: String): HttpUrl {
        val urlBuilder = relativeUrl.newBuilder(path.trimStart { it == '/' })
            ?: throw MalformedURLException()
        return urlBuilder.build()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        other as RestClient
        return server == other.server && httpClient == other.httpClient
    }

    override fun hashCode(): Int = Objects.hash(server, httpClient)

    override fun toString(): String = "RestClient{serverConfig=$server, httpClient=$httpClient}"

    /** Create a new builder with the settings of the current client.  */
    fun withConfiguration(builder: Builder.() -> Unit): RestClient = Builder(httpClient).apply {
        this.server = this@RestClient.server
        builder()
    }.build()

    /** Builder.  */
    class Builder internal constructor(
        val httpClientBuilder: OkHttpClient.Builder
    ) {
        var server: ServerConfig? = null
            set(value) {
                if (value != null) {
                    if (value.isUnsafe) {
                        checkNotNull(RestUtils.UNSAFE_SSL_FACTORY) {
                            "Cannot use unsafe connection, it is disallowed by the runtime environment."
                        }
                        httpClientBuilder.sslSocketFactory(
                            RestUtils.UNSAFE_SSL_FACTORY,
                            RestUtils.UNSAFE_TRUST_MANAGER[0] as X509TrustManager
                        )
                        httpClientBuilder.hostnameVerifier(RestUtils.UNSAFE_HOSTNAME_VERIFIER)
                    } else {
                        val trustManager = RestUtils.systemDefaultTrustManager()
                        val socketFactory = RestUtils.systemDefaultSslSocketFactory(trustManager)
                        httpClientBuilder.sslSocketFactory(socketFactory, trustManager)
                        httpClientBuilder.hostnameVerifier(RestUtils.DEFAULT_HOSTNAME_VERIFIER)
                    }
                }
                field = value
            }
        var headers: Headers = headersOf()

        constructor(client: OkHttpClient) : this(client.newBuilder())

        /** Allowed protocols.  */
        fun protocols(protocols: List<Protocol>) {
            httpClientBuilder.protocols(protocols)
        }

        /** Whether to enable GZIP compression.  */
        fun gzipCompression(compression: Boolean) {
            val gzip = httpClientBuilder.interceptors()
                .find { it is GzipRequestInterceptor} as GzipRequestInterceptor?
            if (compression && gzip == null) {
                logger.debug("Enabling GZIP compression")
                httpClientBuilder.addInterceptor(GzipRequestInterceptor())
            } else if (!compression && gzip != null) {
                logger.debug("Disabling GZIP compression")
                httpClientBuilder.interceptors().remove(gzip)
            }
        }

        /** Timeouts for connecting, reading and writing.  */
        fun timeout(timeout: Long, unit: TimeUnit) {
            httpClientBuilder.connectTimeout(timeout, unit)
                .readTimeout(timeout, unit)
                .writeTimeout(timeout, unit)
        }

        /** Build a new RestClient.  */
        fun build(): RestClient {
            return RestClient(this)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RestClient::class.java)
        const val DEFAULT_TIMEOUT: Long = 30
        private var globalHttpClientRef = WeakReference<OkHttpClient?>(null)

        val globalHttpClient: OkHttpClient
            @Synchronized
            get() = globalHttpClientRef.get()
                ?: createDefaultClient().build()
                    .also { globalHttpClientRef = WeakReference(it) }

        /** Get the response body of a response as a String.
         * Will return null if the response body is null.
         * @return body contents as a String.
         * @throws IOException if the body could not be read as a String.
         */
        @JvmStatic
        @Throws(IOException::class)
        fun Response.bodyString(): String? {
            return body?.use { it.string() }
        }

        /** Create a builder with a global shared OkHttpClient.  */
        @JvmStatic
        fun globalRestClient(builder: Builder.() -> Unit = {}): RestClient =
            Builder(globalHttpClient).apply(builder).build()

        /** Create a builder with a new OkHttpClient using default settings.  */
        @JvmStatic
        fun newRestClient(builder: Builder.() -> Unit): RestClient =
            Builder(createDefaultClient()).apply(builder).build()

        /**
         * Create a new OkHttpClient. The timeouts are set to the default.
         * @return new OkHttpClient.
         */
        private fun createDefaultClient() = OkHttpClient.Builder()
            .connectTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
    }
}

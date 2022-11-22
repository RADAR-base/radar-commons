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
package org.radarbase.util

import okhttp3.internal.platform.Platform
import okhttp3.internal.tls.OkHostnameVerifier
import org.slf4j.LoggerFactory
import java.security.GeneralSecurityException
import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.*

/** Utility methods and variables for OkHttp initialization.  */
object RestUtils {
    private val logger = LoggerFactory.getLogger(RestUtils::class.java)

    /** OkHttp3 default hostname verifier.  */
    val DEFAULT_HOSTNAME_VERIFIER: HostnameVerifier = OkHostnameVerifier

    /** OkHttp3 hostname verifier for unsafe connections.  */
    val UNSAFE_HOSTNAME_VERIFIER = HostnameVerifier { _, _ -> true }

    /** Unsafe OkHttp3 trust manager that trusts all certificates.  */
    val UNSAFE_TRUST_MANAGER = arrayOf<TrustManager>(
        object : X509TrustManager {
            override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) = Unit

            override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) = Unit

            override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
        }
    )

    /** Unsafe OkHttp3 SSLSocketFactory that trusts all certificates.  */
    val UNSAFE_SSL_FACTORY: SSLSocketFactory?

    init {
        val factory: SSLSocketFactory? = try {
            SSLContext.getInstance("SSL").apply {
                init(null, UNSAFE_TRUST_MANAGER, SecureRandom())
            }.socketFactory
        } catch (e: GeneralSecurityException) {
            logger.error("Failed to initialize unsafe SSL factory", e)
            null
        }
        UNSAFE_SSL_FACTORY = factory
    }

    /**
     * Default OkHttp3 trust manager that trusts all certificates.
     * Copied from private method in OkHttpClient.
     */
    fun systemDefaultTrustManager(): X509TrustManager {
        return try {
            val trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm()
            )
            trustManagerFactory.init(null as KeyStore?)
            val trustManagers = trustManagerFactory.trustManagers
            check(trustManagers.size == 1 && trustManagers[0] is X509TrustManager) {
                "Unexpected default trust managers:" + trustManagers.contentToString()
            }
            trustManagers[0] as X509TrustManager
        } catch (e: GeneralSecurityException) {
            throw IllegalStateException("No System TLS", e)
        }
    }

    /**
     * Default OkHttp3 SSLSocketFactory that trusts all certificates.
     * Copied from private method in OkHttpClient.
     */
    fun systemDefaultSslSocketFactory(trustManager: X509TrustManager): SSLSocketFactory {
        return try {
            val sslContext: SSLContext = Platform.get().newSSLContext()
            sslContext.init(null, arrayOf<TrustManager>(trustManager), null)
            sslContext.socketFactory
        } catch (e: GeneralSecurityException) {
            throw IllegalStateException("No System TLS", e)
        }
    }
}

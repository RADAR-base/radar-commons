package org.radarbase.producer.io

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import java.security.cert.X509Certificate
import java.time.Duration
import javax.net.ssl.X509TrustManager

fun HttpClientConfig<*>.timeout(duration: Duration) {
    install(HttpTimeout) {
        val millis = duration.toMillis()
        connectTimeoutMillis = millis
        socketTimeoutMillis = millis
        requestTimeoutMillis = millis
    }
}

fun HttpClientConfig<*>.unsafeSsl() {
    engine {
        if (this is CIOEngineConfig) {
            https {
                trustManager = UNSAFE_TRUST_MANAGER
            }
        }
    }
}

/** Unsafe trust manager that trusts all certificates.  */
private val UNSAFE_TRUST_MANAGER = object : X509TrustManager {
    override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) = Unit

    override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) = Unit

    override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
}

package org.radarbase.producer.io

import io.ktor.client.HttpClientConfig
import io.ktor.client.engine.cio.CIOEngineConfig
import io.ktor.client.plugins.HttpTimeout
import java.security.cert.X509Certificate
import javax.net.ssl.X509TrustManager
import kotlin.time.Duration

fun HttpClientConfig<*>.timeout(duration: Duration) {
    install(HttpTimeout) {
        val millis = duration.inWholeMilliseconds
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

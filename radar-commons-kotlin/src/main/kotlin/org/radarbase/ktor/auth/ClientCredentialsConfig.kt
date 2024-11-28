package org.radarbase.ktor.auth

import org.radarbase.kotlin.util.removeSensitive

data class ClientCredentialsConfig(
    val tokenUrl: String,
    val clientId: String? = null,
    val clientSecret: String? = null,
    val scope: String? = null,
    val audience: String? = null,
) {
    /**
     * Fill in the client ID and client secret from environment variables. The variables are
     * `&lt;prefix&gt;_CLIENT_ID` and `&lt;prefix&gt;_CLIENT_SECRET`.
     */
    fun copyWithEnv(prefix: String = "MANAGEMENT_PORTAL"): ClientCredentialsConfig {
        var result = this
        val envClientId = System.getenv("${prefix}_CLIENT_ID")
        if (envClientId != null) {
            result = result.copy(clientId = envClientId)
        }
        val envClientSecret = System.getenv("${prefix}_CLIENT_SECRET")
        if (envClientSecret != null) {
            result = result.copy(clientSecret = envClientSecret)
        }
        return result
    }

    override fun toString(): String =
        "ClientCredentialsConfig(tokenUrl='$tokenUrl', clientId=$clientId, clientSecret=${clientSecret.removeSensitive()})"
}

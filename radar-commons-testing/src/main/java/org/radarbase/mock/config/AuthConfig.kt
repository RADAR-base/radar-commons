package org.radarbase.mock.config

import com.fasterxml.jackson.annotation.JsonProperty

class AuthConfig {
    @JsonProperty("client_id")
    var clientId: String? = null

    @JsonProperty("client_secret")
    var clientSecret: String? = null

    @JsonProperty("token_url")
    var tokenUrl: String? = null

    /**
     * Fill in the client ID and client secret from environment variables. The variables are
     * `&lt;prefix&gt;_CLIENT_ID` and `&lt;prefix&gt;_CLIENT_SECRET`.
     */
    fun withEnv(prefix: String) {
        val envClientId = System.getenv(prefix + "_CLIENT_ID")
        if (envClientId != null) {
            clientId = envClientId
        }
        val envClientSecret = System.getenv(prefix + "_CLIENT_SECRET")
        if (envClientSecret != null) {
            clientSecret = envClientSecret
        }
    }

    override fun toString(): String {
        return (
            "AuthConfig{" +
                "clientId='" + clientId + '\'' +
                ", clientSecret='******'" +
                ", tokenUrl='" + tokenUrl + '\'' +
                '}'
            )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        other as AuthConfig
        return clientId == other.clientId &&
            clientSecret == other.clientSecret &&
            tokenUrl == other.tokenUrl
    }

    override fun hashCode(): Int = clientId.hashCode()
}

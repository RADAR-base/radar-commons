package org.radarbase.mock.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class AuthConfig {
    @JsonProperty("client_id")
    private String clientId;

    @JsonProperty("client_secret")
    private String clientSecret;

    @JsonProperty("token_url")
    private String tokenUrl;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getTokenUrl() {
        return tokenUrl;
    }

    public void setTokenUrl(String tokenUrl) {
        this.tokenUrl = tokenUrl;
    }

    public void withEnv(String prefix) {
        String envClientId = System.getenv(prefix + "_CLIENT_ID");
        if (envClientId != null) {
            this.clientId = envClientId;
        }
        String envClientSecret = System.getenv(prefix + "_CLIENT_SECRET");
        if (envClientSecret != null) {
            this.clientSecret = envClientSecret;
        }
    }

    @Override
    public String toString() {
        return "AuthConfig{"
                + "clientId='" + clientId + '\''
                + ", clientSecret='******'"
                + ", tokenUrl='" + tokenUrl + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AuthConfig that = (AuthConfig) o;

        return Objects.equals(clientId, that.clientId)
                && Objects.equals(clientSecret, that.clientSecret)
                && Objects.equals(tokenUrl, that.tokenUrl);
    }

    @Override
    public int hashCode() {
        return clientId != null ? clientId.hashCode() : 0;
    }
}

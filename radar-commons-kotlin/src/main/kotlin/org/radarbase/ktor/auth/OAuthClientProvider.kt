package org.radarbase.ktor.auth

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.AuthProvider
import io.ktor.client.plugins.auth.providers.BearerAuthConfig
import io.ktor.client.plugins.auth.providers.BearerAuthProvider
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.accept
import io.ktor.client.request.forms.submitForm
import io.ktor.client.request.headers
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.Parameters
import io.ktor.http.auth.AuthScheme
import io.ktor.http.auth.HttpAuthHeader
import io.ktor.http.isSuccess
import io.ktor.util.KtorDsl
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.seconds

private val logger = LoggerFactory.getLogger(Auth::class.java)

/**
 * Installs the client's [BearerAuthProvider].
 */
fun Auth.clientCredentials(block: ClientCredentialsAuthConfig.() -> Unit) {
    with(ClientCredentialsAuthConfig().apply(block)) {
        this@clientCredentials.providers.add(ClientCredentialsAuthProvider(_requestToken, _loadTokens, _sendWithoutRequest, realm))
    }
}

fun Auth.clientCredentials(
    authConfig: ClientCredentialsConfig,
    targetHost: String? = null,
): Flow<OAuth2AccessToken?> {
    requireNotNull(authConfig.clientId) { "Missing client ID" }
    requireNotNull(authConfig.clientSecret) { "Missing client secret" }
    val flow = MutableStateFlow<OAuth2AccessToken?>(null)

    clientCredentials {
        if (targetHost != null) {
            sendWithoutRequest { request ->
                request.url.host == targetHost
            }
        }
        requestToken {
            val response = client.submitForm(
                url = authConfig.tokenUrl,
                formParameters = Parameters.build {
                    append("grant_type", "client_credentials")
                    append("client_id", authConfig.clientId)
                    append("client_secret", authConfig.clientSecret)
                    authConfig.scope?.let { append("scope", it) }
                    authConfig.audience?.let { append("audience", it) }
                },
            ) {
                accept(ContentType.Application.Json)
                markAsRequestTokenRequest()
            }
            val refreshTokenInfo: OAuth2AccessToken? = if (!response.status.isSuccess()) {
                logger.error("Failed to fetch new token: {}", response.bodyAsText())
                null
            } else {
                response.body<OAuth2AccessToken>()
            }
            flow.value = refreshTokenInfo
            refreshTokenInfo
        }
    }

    return flow
}

/**
 * Parameters to be passed to [BearerAuthConfig.refreshTokens] lambda.
 */
class RequestTokenParams(
    val client: HttpClient,
) {
    /**
     * Marks that this request is for requesting auth tokens, resulting in a special handling of it.
     */
    fun HttpRequestBuilder.markAsRequestTokenRequest() {
        attributes.put(Auth.AuthCircuitBreaker, Unit)
    }
}

/**
 * A configuration for [BearerAuthProvider].
 */
@KtorDsl
class ClientCredentialsAuthConfig {
    internal var _requestToken: suspend RequestTokenParams.() -> OAuth2AccessToken? = { null }
    internal var _loadTokens: suspend () -> OAuth2AccessToken? = { null }
    internal var _sendWithoutRequest: (HttpRequestBuilder) -> Boolean = { true }

    var realm: String? = null

    /**
     * Configures a callback that refreshes a token when the 401 status code is received.
     */
    fun requestToken(block: suspend RequestTokenParams.() -> OAuth2AccessToken?) {
        _requestToken = block
    }

    /**
     * Configures a callback that loads a cached token from a local storage.
     * Note: Using the same client instance here to make a request will result in a deadlock.
     */
    fun loadTokens(block: suspend () -> OAuth2AccessToken?) {
        _loadTokens = block
    }

    /**
     * Sends credentials without waiting for [HttpStatusCode.Unauthorized].
     */
    fun sendWithoutRequest(block: (HttpRequestBuilder) -> Boolean) {
        _sendWithoutRequest = block
    }
}

/**
 * An authentication provider for the Bearer HTTP authentication scheme.
 * Bearer authentication involves security tokens called bearer tokens.
 * As an example, these tokens can be used as a part of OAuth flow to authorize users of your application
 * by using external providers, such as Google, Facebook, Twitter, and so on.
 *
 * You can learn more from [Bearer authentication](https://ktor.io/docs/bearer-client.html).
 */
class ClientCredentialsAuthProvider(
    private val requestToken: suspend RequestTokenParams.() -> OAuth2AccessToken?,
    loadTokens: suspend () -> OAuth2AccessToken?,
    private val sendWithoutRequestCallback: (HttpRequestBuilder) -> Boolean = { true },
    private val realm: String?,
) : AuthProvider {

    @Suppress("OverridingDeprecatedMember")
    @Deprecated("Please use sendWithoutRequest function instead", replaceWith = ReplaceWith("sendWithoutRequest(request)"))
    override val sendWithoutRequest: Boolean
        get() = error("Deprecated")

    private val tokensHolder = AuthTokenHolder(loadTokens)

    override fun sendWithoutRequest(request: HttpRequestBuilder): Boolean = sendWithoutRequestCallback(request)

    /**
     * Checks if current provider is applicable to the request.
     */
    override fun isApplicable(auth: HttpAuthHeader): Boolean {
        if (auth.authScheme != AuthScheme.Bearer) return false
        if (realm == null) return true
        if (auth !is HttpAuthHeader.Parameterized) return false

        return auth.parameter("realm") == realm
    }

    /**
     * Adds an authentication method headers and credentials.
     */
    override suspend fun addRequestHeaders(request: HttpRequestBuilder, authHeader: HttpAuthHeader?) {
        // Check if token is expired and refresh proactively
        if (tokensHolder.isTokenExpired()) {
            logger.debug("Token is expired, refreshing proactively")
            // Don't attempt proactive refresh if we're already in a token request to avoid infinite loops
            if (request.attributes.contains(Auth.AuthCircuitBreaker)) {
                logger.debug("Already in token request, skipping proactive refresh")
            } else {
                val newToken = tokensHolder.setToken {
                    // We can't use the current client for token refresh due to circular dependency
                    // So we'll skip proactive refresh and let the 401 handling take care of it
                    logger.debug("Skipping proactive refresh due to client dependency, will refresh on 401")
                    null
                }
                if (newToken != null) {
                    request.headers {
                        if (contains(HttpHeaders.Authorization)) {
                            remove(HttpHeaders.Authorization)
                        }
                        append(HttpHeaders.Authorization, "Bearer ${(newToken as? OAuth2AccessToken)?.accessToken}")
                    }
                    return
                }
            }
        }

        val token = tokensHolder.loadToken() ?: return

        request.headers {
            if (contains(HttpHeaders.Authorization)) {
                remove(HttpHeaders.Authorization)
            }
            append(HttpHeaders.Authorization, "Bearer ${(token as? OAuth2AccessToken)?.accessToken}")
        }
    }

    override suspend fun refreshToken(response: HttpResponse): Boolean {
        val newToken = tokensHolder.setToken {
            requestToken(RequestTokenParams(response.call.client))
        }
        return newToken != null
    }

    fun clearToken() {
        tokensHolder.clearToken()
    }
}

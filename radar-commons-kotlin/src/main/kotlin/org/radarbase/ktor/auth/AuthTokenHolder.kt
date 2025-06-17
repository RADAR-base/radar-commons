package org.radarbase.ktor.auth

import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.atomic.AtomicReference

internal class AuthTokenHolder<T>(
    private val loadTokens: suspend () -> T?,
) {
    private val refreshTokensDeferred = AtomicReference<CompletableDeferred<T?>?>(null)
    private val loadTokensDeferred = AtomicReference<CompletableDeferred<T?>?>(null)
    private val tokenCreationTime = AtomicReference<Long?>(null)

    internal fun clearToken() {
        loadTokensDeferred.set(null)
        refreshTokensDeferred.set(null)
        tokenCreationTime.set(null)
    }

    internal suspend fun loadToken(): T? {
        var deferred: CompletableDeferred<T?>?
        do {
            deferred = loadTokensDeferred.get()
            val newValue = deferred ?: CompletableDeferred()
        } while (!loadTokensDeferred.compareAndSet(deferred, newValue))

        return if (deferred != null) {
            deferred.await()
        } else {
            val newTokens = loadTokens()
            if (newTokens != null) {
                tokenCreationTime.set(System.currentTimeMillis())
            }
            loadTokensDeferred.get()!!.complete(newTokens)
            newTokens
        }
    }

    internal suspend fun setToken(block: suspend () -> T?): T? {
        var deferred: CompletableDeferred<T?>?
        do {
            deferred = refreshTokensDeferred.get()
            val newValue = deferred ?: CompletableDeferred()
        } while (!refreshTokensDeferred.compareAndSet(deferred, newValue))

        val newToken = if (deferred == null) {
            val newTokens = block()
            if (newTokens != null) {
                tokenCreationTime.set(System.currentTimeMillis())
            }
            refreshTokensDeferred.get()!!.complete(newTokens)
            refreshTokensDeferred.set(null)
            newTokens
        } else {
            deferred.await()
        }
        loadTokensDeferred.set(CompletableDeferred(newToken))
        return newToken
    }
    
    /**
     * Check if the current token is expired based on its creation time and expires_in value.
     * Only works for OAuth2AccessToken types.
     */
    internal fun isTokenExpired(bufferSeconds: Long = 30): Boolean {
        val currentToken = loadTokensDeferred.get()?.takeIf { it.isCompleted }?.getCompleted()
        val creationTime = tokenCreationTime.get()
        
        if (currentToken == null || creationTime == null) {
            return false // No token or creation time, assume not expired
        }
        
        // Check if it's an OAuth2AccessToken with expiration info
        if (currentToken is OAuth2AccessToken) {
            if (currentToken.expiresIn <= 0) {
                return false // No expiration info, assume valid
            }
            
            val currentTime = System.currentTimeMillis()
            val expirationTime = creationTime + (currentToken.expiresIn * 1000) // Convert to milliseconds
            val bufferTime = bufferSeconds * 1000 // Convert buffer to milliseconds
            
            return currentTime >= (expirationTime - bufferTime)
        }
        
        return false // Not an OAuth2AccessToken, can't determine expiration
    }
}

package org.radarbase.ktor.auth

import io.ktor.client.request.HttpRequestBuilder
import io.ktor.http.HttpHeaders

fun HttpRequestBuilder.bearer(token: String) {
    headers[HttpHeaders.Authorization] = "Bearer $token"
}

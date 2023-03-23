
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.util.*

fun HttpRequestBuilder.basicAuth(username: String, password: String) {
    val credentials = "$username:$password"
        .toByteArray(Charsets.UTF_8)
        .encodeBase64()
    headers[HttpHeaders.Authorization] = "Basic $credentials"
}

fun HttpRequestBuilder.bearer(token: String) {
    headers[HttpHeaders.Authorization] = "Bearer $token"
}

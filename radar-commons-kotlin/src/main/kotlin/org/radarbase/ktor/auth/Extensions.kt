
import io.ktor.client.request.*
import io.ktor.http.*

fun HttpRequestBuilder.bearer(token: String) {
    headers[HttpHeaders.Authorization] = "Bearer $token"
}

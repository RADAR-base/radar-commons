package org.radarbase.producer.schema

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.accept
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.request.url
import io.ktor.http.ContentType
import io.ktor.http.HttpMethod
import io.ktor.http.contentType
import io.ktor.http.isSuccess
import io.ktor.serialization.kotlinx.json.json
import io.ktor.util.reflect.TypeInfo
import io.ktor.util.reflect.typeInfo
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.apache.avro.Schema
import org.radarbase.producer.rest.RestException.Companion.toRestException
import java.io.IOException
import kotlin.coroutines.CoroutineContext

/** REST client for Confluent schema registry.  */
class SchemaRestClient(
    httpClient: HttpClient,
    baseUrl: String,
    private val ioContext: CoroutineContext = Dispatchers.IO,
) {
    private val httpClient: HttpClient = httpClient.config {
        install(ContentNegotiation) {
            json(
                Json {
                    ignoreUnknownKeys = true
                    coerceInputValues = true
                },
            )
        }
        defaultRequest {
            url(baseUrl)
            accept(ContentType.Application.Json)
        }
    }

    suspend inline fun <reified T> request(
        noinline requestBuilder: HttpRequestBuilder.() -> Unit,
    ): T = request(typeInfo<T>(), requestBuilder)

    suspend fun <T> request(
        typeInfo: TypeInfo,
        requestBuilder: HttpRequestBuilder.() -> Unit,
    ): T = withContext(ioContext) {
        val response = httpClient.request {
            requestBuilder()
        }
        if (!response.status.isSuccess()) {
            throw response.toRestException()
        }
        response.body(typeInfo)
    }

    suspend fun requestEmpty(
        requestBuilder: HttpRequestBuilder.() -> Unit,
    ) = withContext(ioContext) {
        val response = httpClient.request {
            requestBuilder()
        }
        if (!response.status.isSuccess()) {
            throw response.toRestException()
        }
    }

    /** Retrieve schema metadata from server.  */
    @Throws(IOException::class)
    suspend fun retrieveSchemaMetadata(
        subject: String,
        version: Int,
    ): ParsedSchemaMetadata {
        val isLatest = version <= 0
        val versionPath = if (isLatest) "latest" else version
        return schemaGet("subjects/$subject/versions/$versionPath")
            .toParsedSchemaMetadata()
    }

    @Throws(IOException::class)
    suspend fun schemaGet(path: String): SchemaMetadata = request {
        method = HttpMethod.Get
        url(path)
    }

    @Throws(IOException::class)
    suspend fun schemaPost(
        path: String,
        schema: Schema,
    ): SchemaMetadata = request {
        method = HttpMethod.Post
        url(path)
        contentType(ContentType.Application.Json)
        setBody(SchemaMetadata(schema = schema.toString()))
    }

    /** Add a schema to a subject.  */
    @Throws(IOException::class)
    suspend fun addSchema(subject: String, schema: Schema): ParsedSchemaMetadata {
        val result = schemaPost("subjects/$subject/versions", schema)
        return ParsedSchemaMetadata(
            id = checkNotNull(result.id) { "Missing schema ID in request result" },
            version = result.version,
            schema = schema,
        )
    }

    /** Request metadata for a schema on a subject.  */
    @Throws(IOException::class)
    suspend fun requestMetadata(
        subject: String,
        schema: Schema,
    ): ParsedSchemaMetadata {
        val result = schemaPost("subjects/$subject", schema)
        return ParsedSchemaMetadata(
            id = checkNotNull(result.id) { "Missing schema ID in request result" },
            version = result.version,
            schema = schema,
        )
    }

    /** Retrieve schema metadata from server.  */
    suspend fun retrieveSchemaById(id: Int): Schema =
        schemaGet("/schemas/ids/$id")
            .toParsedSchemaMetadata(id)
            .schema
}

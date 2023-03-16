package org.radarbase.producer.schema

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.apache.avro.Schema
import org.radarbase.producer.rest.RestException
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
            json(Json {
                ignoreUnknownKeys = true
                coerceInputValues = true
            })
        }
        defaultRequest {
            url(baseUrl)
            accept(ContentType.Application.Json)
        }
    }

    /** Retrieve schema metadata from server.  */
    @Throws(IOException::class)
    suspend fun retrieveSchemaMetadata(
        subject: String,
        version: Int
    ): ParsedSchemaMetadata {
        val isLatest = version <= 0
        val versionPath = if (isLatest) "latest" else version
        return schemaGet("subjects/$subject/versions/$versionPath")
            .toParsedSchemaMetadata()
    }

    @Throws(IOException::class)
    suspend fun schemaGet(path: String): SchemaMetadata = withContext(ioContext) {
        val response = httpClient.get {
            url(path)
        }
        if (response.status.isSuccess()) {
            response.body()
        } else {
            throw RestException(response.status, response.bodyAsText())
        }
    }

    @Throws(IOException::class)
    suspend fun schemaPost(
        path: String,
        schema: Schema
    ): SchemaMetadata = withContext(ioContext) {
        val response = httpClient.post {
            url(path)
            contentType(ContentType.Application.Json)
            setBody(SchemaMetadata(schema = schema.toString()))
        }
        if (response.status.isSuccess()) {
            response.body()
        } else {
            throw RestException(response.status, response.bodyAsText())
        }
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
        schema: Schema
    ): ParsedSchemaMetadata = withContext(ioContext) {
        val result = schemaPost("subjects/$subject", schema)
        ParsedSchemaMetadata(
            id = checkNotNull(result.id) { "Missing schema ID in request result" },
            version = result.version,
            schema = schema
        )
    }

    /** Retrieve schema metadata from server.  */
    suspend fun retrieveSchemaById(id: Int): Schema =
        schemaGet("/schemas/ids/$id")
            .toParsedSchemaMetadata()
            .schema
}

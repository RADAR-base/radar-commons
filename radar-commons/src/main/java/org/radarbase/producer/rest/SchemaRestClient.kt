package org.radarbase.producer.rest

import okhttp3.MediaType
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody
import okio.BufferedSink
import org.apache.avro.Schema
import org.json.JSONException
import org.json.JSONObject
import java.io.IOException

/** REST client for Confluent schema registry.  */
class SchemaRestClient(
    private val client: RestClient,
) {
    /** Retrieve schema metadata from server.  */
    @Throws(JSONException::class, IOException::class)
    fun retrieveSchemaMetadata(subject: String, version: Int): ParsedSchemaMetadata {
        val isLatest = version <= 0
        val node = requestJson(buildString(60) {
            append("/subjects/")
            append(subject)
            append("/versions/")
            if (isLatest) {
                append("latest")
            } else {
                append(version)
            }
        })
        val newVersion = if (isLatest) node.getInt("version") else version
        val schemaId = node.getInt("id")
        val schema = parseSchema(node.getString("schema"))
        return ParsedSchemaMetadata(schemaId, newVersion, schema)
    }

    @Throws(IOException::class)
    private fun requestJson(path: String): JSONObject {
        val request: Request = client.requestBuilder(path)
            .addHeader("Accept", "application/json")
            .build()
        val response = client.requestString(request)
        return JSONObject(response)
    }

    /** Parse a schema from string.  */
    fun parseSchema(schemaString: String): Schema {
        return Schema.Parser().parse(schemaString)
    }

    /** Add a schema to a subject.  */
    @Throws(IOException::class)
    fun addSchema(subject: String, schema: Schema): Int {
        val request: Request = client.requestBuilder("/subjects/$subject/versions")
            .addHeader("Accept", "application/json")
            .post(SchemaRequestBody(schema))
            .build()
        val response = client.requestString(request)
        val node = JSONObject(response)
        return node.getInt("id")
    }

    /** Request metadata for a schema on a subject.  */
    @Throws(IOException::class)
    fun requestMetadata(subject: String, schema: Schema): ParsedSchemaMetadata {
        val request: Request = client.requestBuilder("/subjects/$subject")
            .addHeader("Accept", "application/json")
            .post(SchemaRequestBody(schema))
            .build()
        val response = client.requestString(request)
        val node = JSONObject(response)
        return ParsedSchemaMetadata(
            node.getInt("id"),
            node.getInt("version"), schema
        )
    }

    /** Retrieve schema metadata from server.  */
    @Throws(JSONException::class, IOException::class)
    fun retrieveSchemaById(id: Int): Schema {
        val node = requestJson("/schemas/ids/$id")
        return parseSchema(node.getString("schema"))
    }

    private class SchemaRequestBody(
        private val schema: Schema
    ) : RequestBody() {
        override fun contentType(): MediaType = CONTENT_TYPE

        @Throws(IOException::class)
        override fun writeTo(sink: BufferedSink) {
            sink.write(SCHEMA)
            sink.writeUtf8(JSONObject.quote(schema.toString()))
            sink.writeByte('}'.code)
        }

        companion object {
            private val SCHEMA = "{\"schema\":".toByteArray()
            private val CONTENT_TYPE: MediaType = "application/vnd.schemaregistry.v1+json; charset=utf-8"
                .toMediaType()
        }
    }
}

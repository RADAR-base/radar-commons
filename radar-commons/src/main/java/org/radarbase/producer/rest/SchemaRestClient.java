package org.radarbase.producer.rest;

import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.BufferedSink;
import org.apache.avro.Schema;
import org.json.JSONException;
import org.json.JSONObject;
import org.radarbase.util.Strings;

/** REST client for Confluent schema registry. */
public class SchemaRestClient {
    private final RestClient client;

    public SchemaRestClient(RestClient client) {
        this.client = client;
    }

    /** Retrieve schema metadata from server. */
    public ParsedSchemaMetadata retrieveSchemaMetadata(String subject, int version)
            throws JSONException, IOException {
        boolean isLatest = version <= 0;

        StringBuilder pathBuilder = new StringBuilder(50)
                .append("/subjects/")
                .append(subject)
                .append("/versions/");

        if (isLatest) {
            pathBuilder.append("latest");
        } else {
            pathBuilder.append(version);
        }

        JSONObject node = requestJson(pathBuilder.toString());
        int newVersion = isLatest ? node.getInt("version") : version;
        int schemaId = node.getInt("id");
        Schema schema = parseSchema(node.getString("schema"));
        return new ParsedSchemaMetadata(schemaId, newVersion, schema);
    }

    private JSONObject requestJson(String path) throws IOException {
        Request request = client.requestBuilder(path)
                .addHeader("Accept", "application/json")
                .build();

        String response = client.requestString(request);
        return new JSONObject(response);
    }


    /** Parse a schema from string. */
    public Schema parseSchema(String schemaString) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    /** Add a schema to a subject. */
    public int addSchema(String subject, Schema schema) throws IOException {
        Request request = client.requestBuilder("/subjects/" + subject + "/versions")
                .addHeader("Accept", "application/json")
                .post(new SchemaRequestBody(schema))
                .build();

        String response = client.requestString(request);
        JSONObject node = new JSONObject(response);
        return node.getInt("id");
    }

    /** Request metadata for a schema on a subject. */
    public ParsedSchemaMetadata requestMetadata(String subject, Schema schema)
            throws IOException {
        Request request = client.requestBuilder("/subjects/" + subject)
                .addHeader("Accept", "application/json")
                .post(new SchemaRequestBody(schema))
                .build();

        String response = client.requestString(request);
        JSONObject node = new JSONObject(response);

        return new ParsedSchemaMetadata(node.getInt("id"),
                node.getInt("version"), schema);
    }

    /** Retrieve schema metadata from server. */
    public Schema retrieveSchemaById(int id)
            throws JSONException, IOException {
        JSONObject node = requestJson("/schemas/ids/" + id);
        return parseSchema(node.getString("schema"));
    }

    private static class SchemaRequestBody extends RequestBody {
        private static final byte[] SCHEMA = Strings.utf8("{\"schema\":");
        private static final MediaType CONTENT_TYPE = MediaType.parse(
                "application/vnd.schemaregistry.v1+json; charset=utf-8");

        private final Schema schema;

        private SchemaRequestBody(Schema schema) {
            this.schema = schema;
        }

        @Override
        public MediaType contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public void writeTo(BufferedSink sink) throws IOException {
            sink.write(SCHEMA);
            sink.writeUtf8(JSONObject.quote(schema.toString()));
            sink.writeByte('}');
        }
    }
}

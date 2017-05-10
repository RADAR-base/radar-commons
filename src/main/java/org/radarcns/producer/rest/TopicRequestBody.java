package org.radarcns.producer.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;

/**
 * TopicRequestData in a RequestBody.
 */
class TopicRequestBody extends RequestBody {

    protected final TopicRequestData data;

    TopicRequestBody(TopicRequestData requestData) throws IOException {
        data = requestData;
    }

    @Override
    public MediaType contentType() {
        return RestSender.KAFKA_REST_AVRO_ENCODING;
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        try (OutputStream out = sink.outputStream()) {
            data.writeToStream(out);
        }
    }

    String content() throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            data.writeToStream(out);
            return out.toString();
        }
    }
}

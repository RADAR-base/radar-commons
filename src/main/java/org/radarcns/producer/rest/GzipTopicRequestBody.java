package org.radarcns.producer.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import okio.BufferedSink;

/**
 * TopicRequestData in a Gzipped RequestBody.
 */
class GzipTopicRequestBody extends TopicRequestBody {

    GzipTopicRequestBody(TopicRequestData requestData) throws IOException {
        super(requestData);
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        try (OutputStream out = sink.outputStream();
                GZIPOutputStream gzipOut = new GZIPOutputStream(out)) {
            data.writeToStream(gzipOut);
        }
    }
}

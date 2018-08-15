package org.radarcns.producer.rest;

import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

import static org.radarcns.producer.rest.RestClient.responseBody;
import static org.radarcns.producer.rest.TopicRequestBody.topicRequestContent;

public class UncheckedRequestException extends RuntimeException {
    private static final int LOG_CONTENT_LENGTH = 1024;

    public UncheckedRequestException(String message, IOException cause) {
        super(message, cause);
    }

    public void rethrow() throws IOException {
        if (getCause() instanceof IOException) {
            throw (IOException)getCause();
        } else {
            throw this;
        }
    }

    public static UncheckedRequestException fail(Request request,
            Response response, IOException cause) throws IOException {

        StringBuilder message = new StringBuilder(128);
        message.append("FAILED to transmit message");
        String content;
        if (response != null) {
            message.append(" (HTTP status code ")
                    .append(response.code())
                    .append(')');
            content = responseBody(response);
        } else {
            content = null;
        }

        String requestContent = topicRequestContent(request);
        if (requestContent != null || content != null) {
            message.append(':');
        }

        if (requestContent != null) {
            message.append("\n    ")
                    .append(requestContent, 0,
                            Math.min(requestContent.length(), LOG_CONTENT_LENGTH));
        }

        if (content != null) {
            message.append("\n    ")
                    .append(content);
        }

        return new UncheckedRequestException(message.toString(), cause);
    }
}

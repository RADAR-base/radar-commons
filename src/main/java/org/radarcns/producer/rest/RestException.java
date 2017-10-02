package org.radarcns.producer.rest;

import java.io.IOException;

public class RestException extends IOException {
    private final int statusCode;
    private final String body;

    public RestException(int statusCode, String body) {
        this(statusCode, body, null);
    }

    public RestException(int statusCode, String body, Throwable cause) {
        super("REST call failed (HTTP code " + statusCode + "): "
                + body.substring(0, Math.min(512, body.length())), cause);
        this.statusCode = statusCode;
        this.body = body;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getBody() {
        return body;
    }
}

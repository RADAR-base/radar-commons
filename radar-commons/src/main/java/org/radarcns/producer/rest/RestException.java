/*
 * Copyright 2018 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.producer.rest;

import java.io.IOException;

/**
 * Exception when a HTTP REST request fails.
 */
public class RestException extends IOException {
    private final int statusCode;
    private final String body;

    /**
     * Request with status code and response body.
     * @param statusCode HTTP status code
     * @param body response body.
     */
    public RestException(int statusCode, String body) {
        this(statusCode, body, null);
    }

    /**
     * Request with status code, response body and cause.
     * @param statusCode HTTP status code
     * @param body response body.
     * @param cause causing exception.
     */
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

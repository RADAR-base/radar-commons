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

import static org.radarcns.producer.rest.RestClient.responseBody;
import static org.radarcns.producer.rest.TopicRequestBody.topicRequestContent;

import java.io.IOException;
import okhttp3.Request;
import okhttp3.Response;

/** Unchecked exception for failures during request handling. */
public class UncheckedRequestException extends RuntimeException {
    private static final int LOG_CONTENT_LENGTH = 1024;

    /**
     * Unchecked exception.
     * @param message exception message.
     * @param cause cause of this exception, may be null
     */
    public UncheckedRequestException(String message, IOException cause) {
        super(message, cause);
    }

    /**
     * Rethrow this exception using either its cause, if that is an IOException, or using
     * the current exception.
     * @throws IOException if the cause of the exception was an IOException.
     * @throws UncheckedRequestException if the cause of the exception was not an IOException.
     */
    public void rethrow() throws IOException {
        if (getCause() instanceof IOException) {
            throw (IOException)getCause();
        } else {
            throw new IOException(this);
        }
    }

    /**
     * Create a new UncheckedRequestException based on given call.
     *
     * @param request call request
     * @param response call response, may be null
     * @param cause exception cause, may be null
     * @return new exception
     * @throws IOException if the request or response cannot be constructed into a message.
     */
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

        String requestContent = topicRequestContent(request, LOG_CONTENT_LENGTH);
        if (requestContent != null || content != null) {
            message.append(':');
        }

        if (requestContent != null) {
            message.append("\n    ")
                    .append(requestContent);
        }

        if (content != null) {
            message.append("\n    ")
                    .append(content);
        }

        return new UncheckedRequestException(message.toString(), cause);
    }
}

/*
 * Copyright 2017 The Hyve and King's College London
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

package org.radarbase.producer.rest;

import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okio.BufferedSink;

/**
 * TopicRequestData in a RequestBody.
 */
class TopicRequestBody extends RequestBody {
    protected final RecordRequest<?, ?> data;
    private final MediaType mediaType;

    TopicRequestBody(RecordRequest<?, ?> requestData, MediaType mediaType) {
        this.data = requestData;
        this.mediaType = mediaType;
    }

    @Override
    public MediaType contentType() {
        return mediaType;
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        data.writeToSink(sink);
    }

    static String topicRequestContent(Request request, int maxLength) throws IOException {
        TopicRequestBody body = (TopicRequestBody) request.body();
        if (body == null) {
            return null;
        }
        return body.data.content(maxLength);
    }
}

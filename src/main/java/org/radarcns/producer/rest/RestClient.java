/*
 * Copyright 2017 Kings College London and The Hyve
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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.radarcns.config.ServerConfig;

/** REST client using OkHttp3. */
public class RestClient {
    private final long timeout;
    private final ServerConfig config;
    private final OkHttpClient httpClient;

    /**
     * REST client.
     *
     * @param config server configuration
     * @param connectionTimeout connection timeout in seconds
     */
    public RestClient(ServerConfig config, long connectionTimeout) {
        Objects.requireNonNull(config);
        this.config = config;
        this.timeout = connectionTimeout;

        httpClient = new OkHttpClient.Builder()
                .connectTimeout(connectionTimeout, TimeUnit.SECONDS)
                .writeTimeout(connectionTimeout, TimeUnit.SECONDS)
                .readTimeout(connectionTimeout, TimeUnit.SECONDS)
                .proxy(config.getHttpProxy())
                .build();
    }

    /**
     * Make a blocking request.
     * @param request request, possibly built with {@link #requestBuilder(String)}
     * @return response to the request
     * @throws IOException if the request failes
     * @throws NullPointerException if the request is null
     */
    public Response request(Request request) throws IOException {
        Objects.requireNonNull(request);
        return httpClient.newCall(request).execute();
    }

    /** Configured connection timeout in seconds. */
    public long getTimeout() {
        return timeout;
    }

    /** Configured server. */
    public ServerConfig getConfig() {
        return config;
    }

    /**
     * Create a OkHttp3 request builder with {@link Request.Builder#url(URL)} set.
     * Call{@link Request.Builder#build()} to make the actual request with
     * {@link #request(Request)}.
     *
     * @param relativePath relative path from the server config
     * @return request builder.
     * @throws MalformedURLException if the path not valid
     */
    public Request.Builder requestBuilder(String relativePath) throws MalformedURLException {
        return new Request.Builder().url(getRelativeUrl(relativePath));
    }

    /**
     * Get a URL relative to the configured server.
     * @param path relative path
     * @return URL
     * @throws MalformedURLException if the path is malformed
     */
    public URL getRelativeUrl(String path) throws MalformedURLException {
        while (!path.isEmpty() && path.charAt(0) == '/') {
            path = path.substring(1);
        }
        return new URL(getConfig().getUrl(), path);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RestClient that = (RestClient) o;

        return this.timeout == that.timeout && this.config.equals(that.config);
    }

    @Override
    public int hashCode() {
        int result = (int) (timeout ^ (timeout >>> 32));
        result = 31 * result + config.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RestClient{" +
                "timeout=" + timeout +
                ", config=" + config +
                '}';
    }
}

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
import java.lang.ref.WeakReference;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.radarbase.config.ServerConfig;
import org.radarbase.util.RestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST client using OkHttp3. This class is not thread-safe. */
public class RestClient {
    private static final Logger logger = LoggerFactory.getLogger(RestClient.class);

    public static final long DEFAULT_TIMEOUT = 30;
    private static WeakReference<OkHttpClient> globalHttpClient = new WeakReference<>(null);

    private final ServerConfig server;
    private final OkHttpClient httpClient;

    private RestClient(Builder builder) {
        this.server = Objects.requireNonNull(builder.serverConfig);
        this.httpClient = builder.client.build();
    }

    /** OkHttp client. */
    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    /** Configured connection timeout in seconds. */
    public long getTimeout() {
        return httpClient.connectTimeoutMillis() / 1000;
    }

    /** Configured server. */
    public ServerConfig getServer() {
        return server;
    }

    /**
     * Make a blocking request.
     * @param request request, possibly built with {@link #requestBuilder(String)}
     * @return response to the request
     * @throws IOException if the request fails
     * @throws NullPointerException if the request is null
     */
    public Response request(Request request) throws IOException {
        Objects.requireNonNull(request);
        return httpClient.newCall(request).execute();
    }

    /**
     * Make an asynchronous request.
     * @param request request, possibly built with {@link #requestBuilder(String)}
     * @param callback callback to activate once the request is done.
     */
    public void request(Request request, Callback callback) {
        Objects.requireNonNull(request);
        Objects.requireNonNull(callback);
        httpClient.newCall(request).enqueue(callback);
    }

    /**
     * Make a request to given relative path. This does not set any request properties except the
     * URL.
     * @param relativePath relative path to request
     * @return response to the request
     * @throws IOException if the path is invalid or the request failed.
     */
    public Response request(String relativePath) throws IOException {
        return request(requestBuilder(relativePath).build());
    }

    /**
     * Make a blocking request and return the body.
     * @param request request to make.
     * @return response body string.
     * @throws RestException if no body was returned or an HTTP status code indicating error was
     *                       returned.
     * @throws IOException if the request cannot be completed or the response cannot be read.
     *
     */
    public String requestString(Request request) throws IOException {
        try (Response response = request(request)) {
            String bodyString = responseBody(response);

            if (!response.isSuccessful() || bodyString == null) {
                throw new RestException(response.code(), bodyString);
            }

            return bodyString;
        }
    }

    /**
     * Create a OkHttp3 request builder with {@link Request.Builder#url(HttpUrl)} set.
     * Call{@link Request.Builder#build()} to make the actual request with
     * {@link #request(Request)}.
     *
     * @param relativePath relative path from the server serverConfig
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
    public HttpUrl getRelativeUrl(String path) throws MalformedURLException {
        String strippedPath = path;
        while (!strippedPath.isEmpty() && strippedPath.charAt(0) == '/') {
            strippedPath = strippedPath.substring(1);
        }
        HttpUrl.Builder builder = getServer().getHttpUrl().newBuilder(strippedPath);
        if (builder == null) {
            throw new MalformedURLException();
        }
        return builder.build();
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

        return this.server.equals(that.server) && this.httpClient.equals(that.httpClient);
    }

    @Override
    public int hashCode() {
        return Objects.hash(server, httpClient);
    }

    @Override
    public String toString() {
        return "RestClient{serverConfig=" + server + ", httpClient=" + httpClient + '}';
    }

    /** Get the response body of a response as a String.
     * Will return null if the response body is null.
     * @param response call response
     * @return body contents as a String.
     * @throws IOException if the body could not be read as a String.
     */
    public static String responseBody(Response response) throws IOException {
        ResponseBody body = response.body();
        if (body == null) {
            return null;
        }
        return body.string();
    }

    /** Create a new builder with the settings of the current client. */
    public Builder newBuilder() {
        return new Builder(httpClient)
                .server(server);
    }

    /** Builder. */
    public static class Builder {
        private ServerConfig serverConfig;
        private final OkHttpClient.Builder client;

        public Builder(OkHttpClient client) {
            this(client.newBuilder());
        }

        public Builder(OkHttpClient.Builder client) {
            this.client = client;
        }

        /** Server configuration. */
        public Builder server(ServerConfig config) {
            this.serverConfig = Objects.requireNonNull(config);

            if (config.isUnsafe()) {
                this.client.sslSocketFactory(RestUtils.UNSAFE_SSL_FACTORY,
                        (X509TrustManager) RestUtils.UNSAFE_TRUST_MANAGER[0]);
                this.client.hostnameVerifier(RestUtils.UNSAFE_HOSTNAME_VERIFIER);
            } else {
                X509TrustManager trustManager = RestUtils.systemDefaultTrustManager();
                SSLSocketFactory socketFactory = RestUtils.systemDefaultSslSocketFactory(trustManager);
                this.client.sslSocketFactory(socketFactory, trustManager);
                this.client.hostnameVerifier(RestUtils.DEFAULT_HOSTNAME_VERIFIER);
            }
            return this;
        }

        /** Allowed protocols. */
        public Builder protocols(List<Protocol> protocols) {
            this.client.protocols(protocols);
            return this;
        }

        /** Builder to extend the HTTP client with. */
        public OkHttpClient.Builder httpClientBuilder() {
            return client;
        }

        /** Whether to enable GZIP compression. */
        public Builder gzipCompression(boolean compression) {
            GzipRequestInterceptor gzip = null;
            for (Interceptor interceptor : client.interceptors()) {
                if (interceptor instanceof GzipRequestInterceptor) {
                    gzip = (GzipRequestInterceptor) interceptor;
                    break;
                }
            }
            if (compression && gzip == null) {
                logger.debug("Enabling GZIP compression");
                client.addInterceptor(new GzipRequestInterceptor());
            } else if (!compression && gzip != null) {
                logger.debug("Disabling GZIP compression");
                client.interceptors().remove(gzip);
            }
            return this;
        }

        /** Timeouts for connecting, reading and writing. */
        public Builder timeout(long timeout, TimeUnit unit) {
            client.connectTimeout(timeout, unit)
                    .readTimeout(timeout, unit)
                    .writeTimeout(timeout, unit);
            return this;
        }

        /** Build a new RestClient. */
        public RestClient build() {
            return new RestClient(this);
        }
    }

    /** Create a builder with a global shared OkHttpClient. */
    public static synchronized RestClient.Builder global() {
        OkHttpClient client = globalHttpClient.get();
        if (client == null) {
            client = createDefaultClient().build();
            globalHttpClient = new WeakReference<>(client);
        }
        return new RestClient.Builder(client);
    }

    /** Create a builder with a new OkHttpClient using default settings. */
    public static synchronized RestClient.Builder newClient() {
        return new RestClient.Builder(createDefaultClient());
    }

    /**
     * Create a new OkHttpClient. The timeouts are set to the default.
     * @return new OkHttpClient.
     */
    private static OkHttpClient.Builder createDefaultClient() {
        return new OkHttpClient.Builder()
                .connectTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
                .writeTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
    }
}

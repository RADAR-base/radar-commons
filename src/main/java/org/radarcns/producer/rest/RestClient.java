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

package org.radarcns.producer.rest;

import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.radarcns.config.ServerConfig;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** REST client using OkHttp3. This class is not thread-safe. */
public class RestClient implements Closeable {
    private final long timeout;
    private final ServerConfig config;
    private final OkHttpClient httpClient;
    private final ManagedConnectionPool connectionPool;
    private boolean isClosed;

    /**
     * REST client. This client will use the OkHttp3 default connection pool and 30 second timeout.
     *
     * @param config server configuration
     */
    public RestClient(ServerConfig config) {
        this(config, 10, null);
    }

    /**
     * REST client. This client will reuse a global connection pool unless
     *
     * @param config server configuration
     * @param connectionTimeout connection timeout in seconds
     * @param connectionPool connection pool to use. If null, default OkHttp3 connection pool will
     *                       be used. Otherwise, given pool will be closed when this client is
     *                       closed.
     */
    public RestClient(ServerConfig config, long connectionTimeout,
            ManagedConnectionPool connectionPool) {
        Objects.requireNonNull(config);
        this.config = config;
        this.timeout = connectionTimeout;
        this.connectionPool = connectionPool;
        this.isClosed = false;

        OkHttpClient.Builder builder;
        if (config.isUnsafe()) {
            try {
                builder = getUnsafeBuilder();
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new IllegalStateException(
                        "Failed to create unsafe SSL certificate connection", e);
            }
        } else {
            builder = new OkHttpClient.Builder();
        }
        httpClient = buildHttpClient(builder, connectionPool);
    }

    /**
     * Generate a {@code OkHttpClient.Builder} to establish connection with server using
     *      self-signed certificate.
     *
     * @return builder to create an {@code OkHttpClient}
     * @throws NoSuchAlgorithmException if the required cryptographic algorithm is not available
     * @throws KeyManagementException if key management fails
     */
    public static OkHttpClient.Builder getUnsafeBuilder()
        throws NoSuchAlgorithmException, KeyManagementException {
        final TrustManager[] trustAllCerts = new TrustManager[] {
            new X509TrustManager() {
                @Override
                public void checkClientTrusted(java.security.cert.X509Certificate[] chain,
                        String authType) throws CertificateException {
                    //Nothing to do
                }

                @Override
                public void checkServerTrusted(java.security.cert.X509Certificate[] chain,
                        String authType) throws CertificateException {
                    //Nothing to do
                }

                @Override
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return new java.security.cert.X509Certificate[]{};
                }
            }
        };

        final SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0]);
        builder.hostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        });

        return builder;
    }

    /**
     * Build a OkHttpClient setting timeouts, connection pool and proxy.
     *
     * @param builder builder useful to provide extra configuration
     * @param connectionPool connection pool to use. If null, default OkHttp3 connection pool will
     *                       be used.
     * @return OkHttpClient client
     */
    private OkHttpClient buildHttpClient(Builder builder,
            ManagedConnectionPool connectionPool) {
        builder
                .connectTimeout(timeout, TimeUnit.SECONDS)
                .writeTimeout(timeout, TimeUnit.SECONDS)
                .readTimeout(timeout, TimeUnit.SECONDS)
                .proxy(config.getHttpProxy())
                .protocols(Collections.singletonList(Protocol.HTTP_1_1));

        if (connectionPool != null) {
            builder.connectionPool(connectionPool.acquire());
        }

        return builder.build();
    }

    /** Configured connection timeout in seconds. */
    public long getTimeout() {
        return timeout;
    }

    /** Configured server. */
    public ServerConfig getConfig() {
        return config;
    }

    /** Connection pool being used. */
    public ManagedConnectionPool getConnectionPool() {
        return connectionPool;
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
    public HttpUrl getRelativeUrl(String path) throws MalformedURLException {
        String strippedPath = path;
        while (!strippedPath.isEmpty() && strippedPath.charAt(0) == '/') {
            strippedPath = strippedPath.substring(1);
        }
        HttpUrl.Builder builder = getConfig().getHttpUrl().newBuilder(strippedPath);
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
        return "RestClient{timeout=" + timeout + ", config=" + config + '}';
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        if (connectionPool != null) {
            connectionPool.release();
        }
    }

    public static String responseBody(Response response) throws IOException {
        ResponseBody body = response.body();
        if (body == null) {
            return null;
        }
        return body.string();
    }
}

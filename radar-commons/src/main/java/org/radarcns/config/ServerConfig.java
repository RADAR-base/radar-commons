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

package org.radarcns.config;

import okhttp3.HttpUrl;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URL;
import java.util.List;
import java.util.Objects;

/**
 * POJO representing a ServerConfig configuration
 */
public class ServerConfig {
    private String host;
    private int port = -1;
    private String protocol;
    private String path = null;
    private String proxyHost;
    private int proxyPort = -1;
    private boolean unsafe = false;

    public ServerConfig() {
        // POJO initializer
    }

    // Parses the server from URL
    public ServerConfig(URL url) {
        host = url.getHost();
        port = url.getPort();
        protocol = url.getProtocol();
        setPath(url.getFile());
    }

    // Parses the server from URL
    public ServerConfig(String urlString) throws MalformedURLException {
        this(new URL(urlString));
    }

    /** Get the path of the server as a string. This does not include proxyHost information. */
    public String getUrlString() {
        return addUrlString(new StringBuilder(40)).toString();
    }

    private StringBuilder addUrlString(StringBuilder builder) {
        if (protocol != null) {
            builder.append(protocol).append("://");
        }
        builder.append(host);
        if (port != -1) {
            builder.append(':').append(port);
        }
        if (path != null) {
            builder.append(path);
        }
        return builder;
    }

    /** Get the paths of a list of servers, concatenated with commas. */
    public static String getPaths(List<ServerConfig> configList) {
        StringBuilder builder = new StringBuilder(configList.size() * 40);
        boolean first = true;
        for (ServerConfig server : configList) {
            if (first) {
                first = false;
            } else {
                builder.append(',');
            }
            server.addUrlString(builder);
        }
        return builder.toString();
    }

    /**
     * Get the server as a URL.
     *
     * @return URL to the server.
     * @throws IllegalStateException if the URL is invalid
     */
    public URL getUrl() {
        try {
            return new URL(protocol, host, port, path == null ? "" : path);
        } catch (MalformedURLException ex) {
            throw new IllegalStateException("Already parsed a URL but it turned out invalid", ex);
        }
    }

    /**
     * Get the server as an HttpUrl.
     * @return HttpUrl to the server
     * @throws IllegalStateException if the URL is invalid
     */
    public HttpUrl getHttpUrl() {
        HttpUrl.Builder urlBuilder = new HttpUrl.Builder()
                .scheme(protocol)
                .host(host);

        if (port != -1) {
            urlBuilder.port(port);
        }
        if (path != null) {
            urlBuilder.encodedPath(path);
        }

        return urlBuilder.build();
    }

    /**
     * Get the HTTP proxyHost associated to given server
     * @return http proxyHost if specified, or null if none is specified.
     * @throws IllegalStateException if proxyHost is set but proxyPort is not or if the server
     *                               protocol is not HTTP(s)
     */
    public Proxy getHttpProxy() {
        if (proxyHost == null) {
            return null;
        } else if (proxyPort == -1) {
            throw new IllegalStateException("proxy_port is not specified for server "
                    + getUrlString() + " with proxyHost");
        }
        if (protocol != null
                && !protocol.equalsIgnoreCase("http")
                && !protocol.equalsIgnoreCase("https")) {
            throw new IllegalStateException(
                    "Server is not an HTTP(S) server, so it cannot use a HTTP proxyHost.");
        }
        return new Proxy(Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
    }

    /** Server host name or IP address. */
    public String getHost() {
        return host;
    }

    /** Set server host name or IP address. */
    public void setHost(String host) {
        this.host = host;
    }

    /** Server port. Defaults to -1. */
    public int getPort() {
        return port;
    }

    /** Set server port. */
    public void setPort(int port) {
        this.port = port;
    }

    /** Server protocol. */
    public String getProtocol() {
        return protocol;
    }

    /** Set server protocol. */
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /** Proxy host name. Null if not set. */
    public String getProxyHost() {
        return proxyHost;
    }

    /** Set proxyHost host name. */
    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    /** Proxy port. Defaults to -1. */
    public int getProxyPort() {
        return proxyPort;
    }

    /** Set proxyHost port. */
    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getPath() {
        return path;
    }

    public final void setPath(String path) {
        if (path == null) {
            this.path = "/";
        } else if (path.contains("?")) {
            throw new IllegalArgumentException("Cannot set server path with query string");
        } else {
            this.path = path.trim();
            if (this.path.isEmpty()) {
                this.path = "/";
            } else {
                if (this.path.charAt(0) != '/') {
                    this.path = '/' + this.path;
                }
                if (this.path.charAt(this.path.length() - 1) != '/') {
                    this.path += '/';
                }
            }
        }
    }

    @Override
    public String toString() {
        return getUrlString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ServerConfig otherConfig = (ServerConfig) other;
        return Objects.equals(host, otherConfig.host)
                && port == otherConfig.port
                && unsafe == otherConfig.unsafe
                && Objects.equals(protocol, otherConfig.protocol)
                && Objects.equals(proxyHost, otherConfig.proxyHost)
                && proxyPort == otherConfig.proxyPort;
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocol, host, port);
    }

    public boolean isUnsafe() {
        return unsafe;
    }

    public void setUnsafe(boolean unsafe) {
        this.unsafe = unsafe;
    }
}

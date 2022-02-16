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

package org.radarbase.config;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import okhttp3.HttpUrl;

/**
 * POJO representing a ServerConfig configuration.
 */
@SuppressWarnings("PMD.GodClass")
public class ServerConfig {
    private static final Pattern URL_PATTERN = Pattern.compile(
            "(?:(?<protocol>\\w+)://)?(?<host>[^:/]+)(?::(?<port>\\d+))?(?<path>/.*)?");

    private String host;
    private int port = -1;
    private String protocol;
    private String path = null;
    private String proxyHost;
    private int proxyPort = -1;
    private boolean unsafe = false;

    /** Pojo initializer. */
    public ServerConfig() {
        // POJO initializer
    }

    /** Parses the config from a URL. */
    public ServerConfig(URL url) {
        host = url.getHost();
        port = url.getPort();
        protocol = url.getProtocol();
        setPath(url.getFile());
    }

    /** Parses the config from a URL string. */
    public ServerConfig(String urlString) throws MalformedURLException {
        Matcher matcher = URL_PATTERN.matcher(urlString);
        if (!matcher.matches()) {
            throw new MalformedURLException("Cannot create URL from string " + urlString);
        }
        protocol = matcher.group("protocol");
        host = matcher.group("host");
        String portString = matcher.group("port");
        if (portString != null && !portString.isEmpty()) {
            port = Integer.parseInt(portString);
        }
        setPath(matcher.group("path"));
    }

    /** Get the path of the server as a string. This does not include proxyHost information. */
    public String getUrlString() {
        StringBuilder builder = new StringBuilder(host.length()
                + (path != null ? path.length() : 0) + 20);
        appendUrlString(builder);
        return builder.toString();
    }

    /** Get the path of the server as a string. This does not include proxyHost information. */
    private void appendUrlString(StringBuilder builder) {
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
            server.appendUrlString(builder);
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
        if (protocol == null || host == null) {
            throw new IllegalStateException("Cannot create URL without protocol and host");
        }
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
        if (protocol == null) {
            protocol = "http";
        }
        return HttpUrl.get(getUrlString());
    }

    /**
     * Get the HTTP proxyHost associated to given server.
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

    /**
     * Set the absolute path. If the path is empty, it will be set to the root. The path
     * will be ended with a single slash. The path will be prepended with a single slash if needed.
     * @param path path string
     * @throws IllegalArgumentException if the path contains a question mark.
     */
    public final void setPath(String path) {
        this.path = cleanPath(path);
    }

    @SuppressWarnings("PMD.UseStringBufferForStringAppends")
    private static String cleanPath(String path) {
        if (path == null) {
            return null;
        }
        if (path.contains("?") || path.contains("#")) {
            throw new IllegalArgumentException("Cannot set server path with query string");
        }
        String newPath = path.trim();
        if (newPath.isEmpty()) {
            return "/";
        }
        if (newPath.charAt(0) != '/') {
            newPath = '/' + newPath;
        }
        if (newPath.charAt(newPath.length() - 1) != '/') {
            newPath += '/';
        }
        return newPath;
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

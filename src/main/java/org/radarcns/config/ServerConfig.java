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

package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.net.URL;
import java.util.List;

/**
 * POJO representing a ServerConfig configuration
 */
public class ServerConfig {
    private String host;
    private int port = -1;
    private String protocol;
    @JsonProperty("proxy")
    private String proxy;
    @JsonProperty("proxy_port")
    private int proxyPort = -1;

    /** Get the path of the server as a string. This does not include proxy information. */
    public String getPath() {
        if (protocol != null) {
            return protocol + "://" + host + ":" + port;
        } else {
            return host + ":" + port;
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
            builder.append(server.getPath());
        }
        return builder.toString();
    }

    /**
     * Get the server as a URL.
     *
     * @return URL to the server.
     * @throws MalformedURLException if protocol is not set or the host name is invalid.
     */
    public URL getURL() throws MalformedURLException {
        return new URL(protocol, host, port, "");
    }

    /**
     * Get the HTTP proxy associated to given server
     * @return http proxy if specified, or null if none is specified.
     * @throws IllegalStateException if proxy is set but proxyPort is not or if the server protocol
     *                               is not HTTP(s)
     */
    public Proxy getHttpProxy() {
        if (proxy == null) {
            return null;
        } else if (proxyPort == -1) {
            throw new IllegalStateException("proxy_port is not specified for server " + getPath()
                    + " with proxy");
        }
        if (protocol != null
                && !protocol.equalsIgnoreCase("http")
                && !protocol.equalsIgnoreCase("https")) {
            throw new IllegalStateException(
                    "Server is not an HTTP(S) server, so it cannot use a HTTP proxy.");
        }
        return new Proxy(Type.HTTP, new InetSocketAddress(proxy, proxyPort));
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
    public String getProxy() {
        return proxy;
    }

    /** Set proxy host name. */
    public void setProxy(String proxy) {
        this.proxy = proxy;
    }

    /** Proxy port. Defaults to -1. */
    public int getProxyPort() {
        return proxyPort;
    }

    /** Set proxy port. */
    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    @Override
    public String toString() {
        return getPath();
    }
}

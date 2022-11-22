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
package org.radarbase.config

import java.net.InetSocketAddress
import java.net.MalformedURLException
import java.net.Proxy
import java.net.URL
import java.util.*

/**
 * POJO representing a ServerConfig configuration.
 */
class ServerConfig {
    /** Server host name or IP address.  */
    /** Set server host name or IP address.  */
    var host: String? = null
    /** Server port. Defaults to -1.  */
    /** Set server port.  */
    var port = -1
    /** Server protocol.  */
    /** Set server protocol.  */
    var protocol: String? = null
    /**
     * Set the absolute path. If the path is empty, it will be set to the root. The path
     * will be ended with a single slash. The path will be prepended with a single slash if needed.
     * @throws IllegalArgumentException if the path contains a question mark.
     */
    var path: String = ""
        set(value) {
            field = value.toUrlPath()
        }
    /** Proxy host name. Null if not set.  */
    /** Set proxyHost host name.  */
    var proxyHost: String? = null
    /** Proxy port. Defaults to -1.  */
    /** Set proxyHost port.  */
    var proxyPort = -1
    var isUnsafe = false

    /** Pojo initializer.  */
    constructor() {
        // POJO initializer
    }

    /** Parses the config from a URL.  */
    constructor(url: URL) {
        host = url.host
        port = url.port
        protocol = url.protocol
        path = url.file
    }

    /** Parses the config from a URL string.  */
    constructor(urlString: String) {
        val matcher = URL_PATTERN.matchEntire(urlString)
            ?: throw MalformedURLException("Cannot create URL from string $this")
        val groups = matcher.groups
        protocol = groups[1]?.value ?: "https"
        host = requireNotNull(groups[2]?.value) { "Cannot create URL without host name from $this" }
        port = groups[3]?.value?.toIntOrNull() ?: -1
        path = groups[4]?.value.toUrlPath()
    }

    /** Get the path of the server as a string. This does not include proxyHost information.  */
    val urlString: String
        get() = buildString(host!!.length + path.length + 20) {
            appendUrlString(this)
        }

    /** Get the path of the server as a string. This does not include proxyHost information.  */
    private fun appendUrlString(builder: StringBuilder) = builder.run {
        if (protocol != null) {
            append(protocol)
            append("://")
        }
        append(host)
        if (port != -1) {
            append(':')
            append(port)
        }
        append(path)
    }

    /**
     * Get the server as a URL.
     *
     * @return URL to the server.
     * @throws IllegalStateException if the URL is invalid
     */
    val url: URL
        get() {
            checkNotNull(protocol) { "Cannot create URL without protocol" }
            checkNotNull(host) { "Cannot create URL without host" }
            return try {
                URL(protocol, host, port, path)
            } catch (ex: MalformedURLException) {
                throw IllegalStateException("Already parsed a URL but it turned out invalid", ex)
            }
        }

    /**
     * Get the HTTP proxyHost associated to given server.
     * @return http proxyHost if specified, or null if none is specified.
     * @throws IllegalStateException if proxyHost is set but proxyPort is not or if the server
     * protocol is not HTTP(s)
     */
    val httpProxy: Proxy?
        get() {
            proxyHost ?: return null
            check(proxyPort != -1) { "proxy_port is not specified for server $urlString with proxyHost" }

            check(
                protocol == null ||
                        protocol.equals("http", ignoreCase = true) ||
                        protocol.equals("https", ignoreCase = true)
            ) { "Server is not an HTTP(S) server, so it cannot use a HTTP proxyHost." }
            return Proxy(Proxy.Type.HTTP, InetSocketAddress(proxyHost, proxyPort))
        }

    override fun toString(): String = urlString

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        other as ServerConfig
        return host == other.host &&
                port == other.port &&
                isUnsafe == other.isUnsafe &&
                protocol == other.protocol &&
                proxyHost == other.proxyHost &&
                proxyPort == other.proxyPort
    }

    override fun hashCode(): Int {
        return Objects.hash(host, path)
    }

    companion object {
        private val URL_PATTERN = "(?:(\\w+)://)?([^:/]+)(?::(\\d+))?(/.*)?".toRegex()
        private val BAD_SLASHES_REGEX = "/(\\.*/)+".toRegex()

        /** Get the paths of a list of servers, concatenated with commas.  */
        @JvmStatic
        fun getPaths(configList: List<ServerConfig>): String = buildString(configList.size * 40) {
            var first = true
            for (server in configList) {
                if (first) {
                    first = false
                } else {
                    append(',')
                }
                server.appendUrlString(this)
            }
        }

        private fun String?.toUrlPath(): String {
            this ?: return ""
            require(!contains("?")) { "Cannot set server path with query string" }
            require(!contains("#")) { "Cannot set server path with location string" }
            var newPath = trim { it <= ' ' }
            if (newPath.isEmpty()) {
                return "/"
            }
            if (newPath.first() != '/') {
                newPath = "/$newPath"
            }
            if (newPath.last() != '/') {
                newPath += '/'
            }
            return newPath.replace(BAD_SLASHES_REGEX, "/")
        }
    }
}

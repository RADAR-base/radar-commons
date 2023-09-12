package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter

/** Mixin class to load ServerConfig files with.  */
abstract class ServerConfigMixin {
    @get:JsonProperty("proxy_host")
    abstract val proxyHost: String?

    @get:JsonProperty("proxy_port")
    abstract val proxyPort: String?

    @JsonSetter("path")
    abstract fun setPath(path: String?)
}

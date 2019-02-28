package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

/** Mixin class to load ServerConfig files with. */
public abstract class ServerConfigMixin {
    /** Proxy host name. Null if not set. */
    @JsonProperty("proxy_host")
    public abstract String getProxyHost();

    @JsonProperty("proxy_port")
    public abstract String getProxyPort();

    @JsonSetter("path")
    public abstract void setPath(String path);
}

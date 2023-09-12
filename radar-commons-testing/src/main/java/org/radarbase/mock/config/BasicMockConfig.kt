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
package org.radarbase.mock.config

import com.fasterxml.jackson.annotation.JsonProperty
import org.radarbase.config.ServerConfig
import org.radarbase.config.ServerConfig.Companion.getPaths

/**
 * A Minimal Mock Config to talk to Kafka and stream data using a mock set-up.
 */
class BasicMockConfig {
    @JsonProperty("producer_mode")
    var producerMode = "rest"

    @JsonProperty("number_of_devices")
    var numberOfDevices = 1
    var broker: List<ServerConfig>? = null

    @JsonProperty("rest_proxy")
    var restProxy: ServerConfig? = null

    @JsonProperty("schema_registry")
    var schemaRegistry: ServerConfig? = null
    var data: List<MockDataConfig>? = null
    private var compression = false

    @JsonProperty("duration_millis")
    var duration = 0L

    @JsonProperty("auth")
    var authConfig: AuthConfig? = null

    val isUnsafeProducer: Boolean
        get() = producerMode.trim { it <= ' ' }
            .equals("unsafe", ignoreCase = true)

    val brokerPaths: String
        get() = getPaths(
            requireNotNull(broker) { "Kafka 'broker' is not configured" },
        )

    fun hasCompression(): Boolean = compression

    fun setCompression(compression: Boolean) {
        this.compression = compression
    }
}

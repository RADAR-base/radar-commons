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

package org.radarcns.mock.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import org.radarcns.config.ServerConfig;

/**
 * A Minimal Mock Config to talk to Kafka and stream data using a mock set-up.
 */
public class BasicMockConfig {
    @JsonProperty("producer_mode")
    private String producerMode = "rest";

    @JsonProperty("number_of_devices")
    private Integer numberOfDevices = 1;

    private List<ServerConfig> broker;

    @JsonProperty("rest_proxy")
    private ServerConfig restProxy;

    @JsonProperty("schema_registry")
    private ServerConfig schemaRegistry;

    private List<MockDataConfig> data = null;

    private boolean compression = false;

    @JsonProperty("duration_millis")
    private long duration = 0L;

    public List<ServerConfig> getBroker() {
        return broker;
    }

    public void setBroker(List<ServerConfig> broker) {
        this.broker = broker;
    }

    public ServerConfig getSchemaRegistry() {
        return schemaRegistry;
    }

    public void setSchemaRegistry(ServerConfig schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public List<MockDataConfig> getData() {
        return data;
    }

    public void setData(List<MockDataConfig> data) {
        this.data = data;
    }

    public String getProducerMode() {
        return producerMode;
    }

    public void setProducerMode(String producerMode) {
        this.producerMode = producerMode;
    }

    public Integer getNumberOfDevices() {
        return numberOfDevices;
    }

    public void setNumberOfDevices(Integer numberOfDevices) {
        this.numberOfDevices = numberOfDevices;
    }

    public ServerConfig getRestProxy() {
        return restProxy;
    }

    public void setRestProxy(ServerConfig restProxy) {
        this.restProxy = restProxy;
    }

    public boolean isDirectProducer() {
        return this.producerMode.trim().equalsIgnoreCase("direct");
    }

    public boolean isUnsafeProducer() {
        return this.producerMode.trim().equalsIgnoreCase("unsafe");
    }

    public String getBrokerPaths() {
        Objects.requireNonNull(broker, "Kafka 'broker' is not configured");
        return ServerConfig.getPaths(broker);
    }

    public boolean hasCompression() {
        return compression;
    }

    public void setCompression(boolean compression) {
        this.compression = compression;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }
}

package org.radarcns.mock;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.radarcns.config.ServerConfig;
import org.radarcns.mock.MockDataConfig;


public class BasicMockConfig {

    private List<ServerConfig> broker;

    private List<ServerConfig> zookeeper;

    @JsonProperty("schema_registry")
    private List<ServerConfig> schemaRegistry;

    private List<MockDataConfig> data;

    public List<ServerConfig> getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(List<ServerConfig> zookeeper) {
        this.zookeeper = zookeeper;
    }

    public List<ServerConfig> getBroker() {
        return broker;
    }

    public void setBroker(List<ServerConfig> broker) {
        this.broker = broker;
    }

    public List<ServerConfig> getSchemaRegistry() {
        return schemaRegistry;
    }

    public void setSchemaRegistry(List<ServerConfig> schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public List<MockDataConfig> getData() {
        return data;
    }

    public void setData(List<MockDataConfig> data) {
        this.data = data;
    }

}

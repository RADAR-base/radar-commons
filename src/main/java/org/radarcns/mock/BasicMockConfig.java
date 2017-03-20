package org.radarcns.mock;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.radarcns.config.ServerConfig;
import org.radarcns.mock.MockDataConfig;

/**
 * A Minimal Mock Config to talk to Kafka and stream data using a mock set-up.
 */
public class BasicMockConfig {

    @JsonProperty("producer_mode")
    private String producerMode;

    @JsonProperty("number_of_devices")
    private Integer numberOfDevices;

    private List<ServerConfig> broker;

    private List<ServerConfig> zookeeper;

    @JsonProperty("rest_proxy")
    private ServerConfig restProxy;

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
        return this.producerMode.trim().equals("direct") ? true : false;
    }

    public String getZookeeperPaths() {
        if (zookeeper == null) {
            throw new IllegalStateException("'zookeeper' is not configured");
        }
        return ServerConfig.getPaths(zookeeper);
    }

    public String getBrokerPaths() {
        if (broker == null) {
            throw new IllegalStateException("Kafka 'broker' is not configured");
        }
        return ServerConfig.getPaths(broker);
    }

    public String getSchemaRegistryPaths() {
        if (schemaRegistry == null) {
            throw new IllegalStateException("'schema_registry' is not configured");
        }

        return ServerConfig.getPaths(schemaRegistry);
    }

    public String getRestProxyPath() {
        if (restProxy == null) {
            throw new IllegalStateException("'rest_proxy' is not configured");
        }

        return restProxy.getPath();
    }
}

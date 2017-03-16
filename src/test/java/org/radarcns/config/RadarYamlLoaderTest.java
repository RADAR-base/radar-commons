package org.radarcns.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.junit.Test;
import org.radarcns.mock.BasicMockConfig;

/**
 * Rest class for {@link RadarYamlLoader}
 */
public class RadarYamlLoaderTest {

    private RadarYamlLoader loader;

    @Test
    public void testBasicLoader() throws IOException {
        String path = "src/test/resources/integration.yml";
        loader = new RadarYamlLoader<BasicMockConfig>();
        loader.load(path, BasicMockConfig.class);

        BasicMockConfig basicConfig = (BasicMockConfig) loader.getProperties();
        assertEquals(basicConfig.getBroker().get(0).toString(), "localhost:9092");
        assertEquals(basicConfig.getZookeeper().get(0).toString(), "localhost:2181");
        assertEquals(basicConfig.getSchemaRegistry().get(0).toString(), "http://localhost:8081");
        assertTrue(basicConfig instanceof BasicMockConfig);
    }

}

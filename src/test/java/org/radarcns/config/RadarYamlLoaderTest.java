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
        assertEquals("localhost:9092", basicConfig.getBroker().get(0).toString());
        assertEquals("localhost:2181", basicConfig.getZookeeper().get(0).toString());
        assertEquals("http://localhost:8081", basicConfig.getSchemaRegistry().get(0).toString());
        assertTrue(basicConfig instanceof BasicMockConfig);
    }

}

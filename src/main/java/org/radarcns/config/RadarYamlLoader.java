package org.radarcns.config;

import static org.radarcns.util.Strings.isNullOrEmpty;

import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Generic YAML Loader that can load Yaml files using {@link ConfigLoader}
 */
public class RadarYamlLoader<T> {

    private T properties;

    private static final Logger log = LoggerFactory.getLogger(RadarYamlLoader.class);


    /**
     * Returns loaded config object
     * @return
     */
    public T getProperties() {
        if (properties == null) {
            throw new IllegalStateException(
                    "Properties cannot be accessed without calling load() first");
        }
        return properties;
    }

    /**
     * Loads file provided by  pathfile into an object of  className.
     * @param pathFile
     * @param className
     * @throws IOException
     */
    public void load(String pathFile, Class<T> className) throws IOException {
        if (properties != null) {
            throw new IllegalStateException("Properties class has been already loaded");
        }

        File file;

        //If pathFile is null
        if (isNullOrEmpty(pathFile)) {
            log.error("File path not provided");
            return;
        } else {
            log.info("USER CONFIGURATION: loading config file at {}", pathFile);
            file = new File(pathFile);
        }

        if (!file.exists()) {
            throw new IllegalArgumentException("Config file " + file + " does not exist");
        }
        if (!file.isFile()) {
            throw new IllegalArgumentException("Config file " + file + " is invalid");
        }

        properties = new ConfigLoader().load(file, className);

    }



}

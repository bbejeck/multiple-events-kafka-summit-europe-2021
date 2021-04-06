package io.confluent.developer.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesLoader {

    public static Properties load(final String filePath) {
        final Properties props = new Properties();
        try (final FileInputStream input = new FileInputStream(filePath)) {
            props.load(input);
            return props;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

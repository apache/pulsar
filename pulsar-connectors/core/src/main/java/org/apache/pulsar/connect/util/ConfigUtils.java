package org.apache.pulsar.connect.util;

import java.util.Properties;

public class ConfigUtils {

    public static long getLong(Properties properties, String key, long defaultValue) {
        final String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (Exception ex) {
                // ignore
            }
        }

        return defaultValue;
    }

    private ConfigUtils() {}
}

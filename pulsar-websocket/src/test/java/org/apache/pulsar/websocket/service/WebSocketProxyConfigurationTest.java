package org.apache.pulsar.websocket.service;

import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.testng.annotations.Test;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class WebSocketProxyConfigurationTest {

    @Test
    public void testConfigFileDefaults() throws Exception {
        try (FileInputStream stream = new FileInputStream("../conf/websocket.conf")) {
            final WebSocketProxyConfiguration javaConfig = PulsarConfigurationLoader.create(new Properties(), WebSocketProxyConfiguration.class);
            final WebSocketProxyConfiguration fileConfig = PulsarConfigurationLoader.create(stream, WebSocketProxyConfiguration.class);
            List<String> toSkip = Arrays.asList("properties", "class");
            int counter = 0;
            for (PropertyDescriptor pd : Introspector.getBeanInfo(WebSocketProxyConfiguration.class).getPropertyDescriptors()) {
                if (pd.getReadMethod() == null || toSkip.contains(pd.getName())) {
                    continue;
                }
                final String key = pd.getName();
                final Object javaValue = pd.getReadMethod().invoke(javaConfig);
                final Object fileValue = pd.getReadMethod().invoke(fileConfig);
                assertTrue(Objects.equals(javaValue, fileValue), "property '"
                        + key + "' conf/websocket.conf default value doesn't match java default value\nConf: "+ fileValue + "\nJava: " + javaValue);
                counter++;
            }
            assertEquals(36, counter);
        }
    }

}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

@Test(groups = "broker")
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
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
package org.apache.pulsar.io.netty;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Tests for Netty Tcp or Udp Source Config
 */
public class NettySourceConfigTest {

    private static final String LOCALHOST = "127.0.0.1";
    private static final String TCP = "tcp";

    @Test
    public void testNettyTcpConfigLoadWithMap() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("type", TCP);
        map.put("host", LOCALHOST);
        map.put("port", 10999);
        map.put("numberOfThreads", 1);

        NettySourceConfig nettySourceConfig = NettySourceConfig.load(map);
        assertNotNull(nettySourceConfig);
        assertEquals(TCP, nettySourceConfig.getType());
        assertEquals(LOCALHOST, nettySourceConfig.getHost());
        assertEquals(10999, nettySourceConfig.getPort());
        assertEquals(1, nettySourceConfig.getNumberOfThreads());
    }

    @Test(expectedExceptions = UnrecognizedPropertyException.class)
    public void testNettyTcpConfigLoadWithMapWhenInvalidPropertyIsSet() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("invalidProperty", 1);

        NettySourceConfig.load(map);
    }

    @Test
    public void testNettyTcpConfigLoadWithYamlFile() throws IOException {
        File yamlFile = getFile("nettySourceConfig.yaml");
        NettySourceConfig nettySourceConfig = NettySourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(nettySourceConfig);
        assertEquals(TCP, nettySourceConfig.getType());
        assertEquals(LOCALHOST, nettySourceConfig.getHost());
        assertEquals(10911, nettySourceConfig.getPort());
        assertEquals(5, nettySourceConfig.getNumberOfThreads());
    }

    @Test(expectedExceptions = UnrecognizedPropertyException.class)
    public void testNettyTcpConfigLoadWithYamlFileWhenInvalidPropertyIsSet() throws IOException {
        File yamlFile = getFile("nettySourceConfigWithInvalidProperty.yaml");
        NettySourceConfig.load(yamlFile.getAbsolutePath());
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }

}

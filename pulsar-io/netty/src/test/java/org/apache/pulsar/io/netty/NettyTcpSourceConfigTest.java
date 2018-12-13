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
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for Netty Tcp Source Config
 */
public class NettyTcpSourceConfigTest {

    private static final String LOCALHOST = "127.0.0.1";

    @Test
    public void testNettyTcpConfigLoadWithMap() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", LOCALHOST);
        map.put("port", 10999);
        map.put("numberOfThreads", 1);

        NettyTcpSourceConfig nettyTcpSourceConfig = NettyTcpSourceConfig.load(map);
        assertNotNull(nettyTcpSourceConfig);
        assertEquals(LOCALHOST, nettyTcpSourceConfig.getHost());
        assertEquals(10999, nettyTcpSourceConfig.getPort());
        assertEquals(1, nettyTcpSourceConfig.getNumberOfThreads());
    }

    @Test(expected = UnrecognizedPropertyException.class)
    public void testNettyTcpConfigLoadWithMapWhenInvalidPropertyIsSet() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("invalidProperty", 1);

        NettyTcpSourceConfig.load(map);
    }

    @Test
    public void testNettyTcpConfigLoadWithYamlFile() throws IOException {
        File yamlFile = getFile("nettyTcpSourceConfig.yaml");
        NettyTcpSourceConfig nettyTcpSourceConfig = NettyTcpSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(nettyTcpSourceConfig);
        assertEquals(LOCALHOST, nettyTcpSourceConfig.getHost());
        assertEquals(10911, nettyTcpSourceConfig.getPort());
        assertEquals(5, nettyTcpSourceConfig.getNumberOfThreads());
    }

    @Test(expected = UnrecognizedPropertyException.class)
    public void testNettyTcpConfigLoadWithYamlFileWhenInvalidPropertyIsSet() throws IOException {
        File yamlFile = getFile("nettyTcpSourceConfigWithInvalidProperty.yaml");
        NettyTcpSourceConfig.load(yamlFile.getAbsolutePath());
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }

}

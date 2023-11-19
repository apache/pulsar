/*
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
package org.apache.pulsar.io.rabbitmq.source;

import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.rabbitmq.RabbitMQSourceConfig;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

/**
 * RabbitMQSourceConfig test
 */
public class RabbitMQSourceConfigTest {
    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sourceConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        RabbitMQSourceConfig config = RabbitMQSourceConfig.load(path);
        assertNotNull(config);
        assertEquals("localhost", config.getHost());
        assertEquals(Integer.parseInt("5672"), config.getPort());
        assertEquals("/", config.getVirtualHost());
        assertEquals("guest", config.getUsername());
        assertEquals("guest", config.getPassword());
        assertEquals("test-queue", config.getQueueName());
        assertEquals("test-connection", config.getConnectionName());
        assertEquals(Integer.parseInt("0"), config.getRequestedChannelMax());
        assertEquals(Integer.parseInt("0"), config.getRequestedFrameMax());
        assertEquals(Integer.parseInt("60000"), config.getConnectionTimeout());
        assertEquals(Integer.parseInt("10000"), config.getHandshakeTimeout());
        assertEquals(Integer.parseInt("60"), config.getRequestedHeartbeat());
        assertEquals(Integer.parseInt("0"), config.getPrefetchCount());
        assertFalse(config.isPrefetchGlobal());
        assertFalse(config.isPassive());
    }

    @Test
    public final void loadFromYamlFileAndContextTest() throws IOException {
        File yamlFile = getFile("sourceConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        SourceContext context = mock(SourceContext.class);
        when(context.getSecret("username")).thenReturn("my-secret-name");
        when(context.getSecret("password")).thenReturn("my-secret-pass");
        RabbitMQSourceConfig config = RabbitMQSourceConfig.load(path, context);
        assertNotNull(config);
        assertEquals(config.getHost(), "localhost");
        assertEquals(config.getPort(), Integer.parseInt("5672"));
        assertEquals(config.getVirtualHost(), "/");
        assertEquals(config.getUsername(), "my-secret-name");
        assertEquals(config.getPassword(), "my-secret-pass");
        assertEquals(config.getQueueName(), "test-queue");
        assertEquals(config.getConnectionName(), "test-connection");
        assertEquals(config.getRequestedChannelMax(), Integer.parseInt("0"));
        assertEquals(config.getRequestedFrameMax(), Integer.parseInt("0"));
        assertEquals(config.getConnectionTimeout(), Integer.parseInt("60000"));
        assertEquals(config.getHandshakeTimeout(), Integer.parseInt("10000"));
        assertEquals(config.getRequestedHeartbeat(), Integer.parseInt("60"));
        assertEquals(config.getPrefetchCount(), Integer.parseInt("0"));
        assertFalse(config.isPrefetchGlobal());
        assertFalse(config.isPassive());
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "localhost");
        map.put("port", "5672");
        map.put("virtualHost", "/");
        map.put("username", "guest");
        map.put("password", "guest");
        map.put("queueName", "test-queue");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("prefetchCount", "0");
        map.put("prefetchGlobal", "false");
        map.put("passive", "true");

        RabbitMQSourceConfig config = RabbitMQSourceConfig.load(map);
        assertNotNull(config);
        assertEquals("localhost", config.getHost());
        assertEquals(Integer.parseInt("5672"), config.getPort());
        assertEquals("/", config.getVirtualHost());
        assertEquals("guest", config.getUsername());
        assertEquals("guest", config.getPassword());
        assertEquals("test-queue", config.getQueueName());
        assertEquals("test-connection", config.getConnectionName());
        assertEquals(Integer.parseInt("0"), config.getRequestedChannelMax());
        assertEquals(Integer.parseInt("0"), config.getRequestedFrameMax());
        assertEquals(Integer.parseInt("60000"), config.getConnectionTimeout());
        assertEquals(Integer.parseInt("10000"), config.getHandshakeTimeout());
        assertEquals(Integer.parseInt("60"), config.getRequestedHeartbeat());
        assertEquals(Integer.parseInt("0"), config.getPrefetchCount());
        assertEquals(false, config.isPrefetchGlobal());
        assertEquals(false, config.isPrefetchGlobal());
        assertEquals(true, config.isPassive());
    }

    @Test
    public final void loadFromMapAndContextTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "localhost");
        map.put("port", "5672");
        map.put("virtualHost", "/");
        map.put("queueName", "test-queue");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("prefetchCount", "0");
        map.put("prefetchGlobal", "false");
        map.put("passive", "true");
        SourceContext context = mock(SourceContext.class);
        when(context.getSecret("username")).thenReturn("my-secret-name");
        when(context.getSecret("password")).thenReturn("my-secret-pass");

        RabbitMQSourceConfig config = RabbitMQSourceConfig.load(map, context);
        assertNotNull(config);
        assertEquals(config.getHost(), "localhost");
        assertEquals(config.getPort(), Integer.parseInt("5672"));
        assertEquals(config.getVirtualHost(), "/");
        assertEquals(config.getUsername(), "my-secret-name");
        assertEquals(config.getPassword(), "my-secret-pass");
        assertEquals(config.getQueueName(), "test-queue");
        assertEquals(config.getConnectionName(), "test-connection");
        assertEquals(config.getRequestedChannelMax(), Integer.parseInt("0"));
        assertEquals(config.getRequestedFrameMax(), Integer.parseInt("0"));
        assertEquals(config.getConnectionTimeout(), Integer.parseInt("60000"));
        assertEquals(config.getHandshakeTimeout(), Integer.parseInt("10000"));
        assertEquals(config.getRequestedHeartbeat(), Integer.parseInt("60"));
        assertEquals(config.getPrefetchCount(), Integer.parseInt("0"));
        assertEquals(config.isPrefetchGlobal(), false);
        assertEquals(config.isPrefetchGlobal(), false);
        assertEquals(config.isPassive(), true);
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "localhost");
        map.put("port", "5672");
        map.put("virtualHost", "/");
        map.put("username", "guest");
        map.put("password", "guest");
        map.put("queueName", "test-queue");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("prefetchCount", "0");
        map.put("prefetchGlobal", "false");
        map.put("passive", "false");

        RabbitMQSourceConfig config = RabbitMQSourceConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = NullPointerException.class,
        expectedExceptionsMessageRegExp = "host property not set.")
    public final void missingHostValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("port", "5672");
        map.put("virtualHost", "/");
        map.put("username", "guest");
        map.put("password", "guest");
        map.put("queueName", "test-queue");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("prefetchCount", "0");
        map.put("prefetchGlobal", "false");
        map.put("passive", "false");

        RabbitMQSourceConfig config = RabbitMQSourceConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "prefetchCount must be non-negative.")
    public final void invalidPrefetchCountTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "localhost");
        map.put("port", "5672");
        map.put("virtualHost", "/");
        map.put("username", "guest");
        map.put("password", "guest");
        map.put("queueName", "test-queue");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("prefetchCount", "-100");
        map.put("prefetchGlobal", "false");
        map.put("passive", "false");

        RabbitMQSourceConfig config = RabbitMQSourceConfig.load(map);
        config.validate();
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}

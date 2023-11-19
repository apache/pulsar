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
package org.apache.pulsar.io.influxdb.v1;

import org.apache.pulsar.io.core.SinkContext;
import org.influxdb.InfluxDB;
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
 * InfluxDBSinkConfig test
 */
public class InfluxDBSinkConfigTest {
    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig-v1.yaml");
        String path = yamlFile.getAbsolutePath();
        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(path);
        assertNotNull(config);
        assertEquals("http://localhost:8086", config.getInfluxdbUrl());
        assertEquals("test_db", config.getDatabase());
        assertEquals("ONE", config.getConsistencyLevel());
        assertEquals("NONE", config.getLogLevel());
        assertEquals("autogen", config.getRetentionPolicy());
        assertEquals(Boolean.parseBoolean("false"), config.isGzipEnable());
        assertEquals(Long.parseLong("1000"), config.getBatchTimeMs());
        assertEquals(Integer.parseInt("100"), config.getBatchSize());
    }

    @Test
    public final void loadFromYamlFileAndContextTest() throws IOException {
        File yamlFile = getFile("sinkConfig-v1.yaml");
        String path = yamlFile.getAbsolutePath();
        SinkContext context = mock(SinkContext.class);
        when(context.getSecret("username")).thenReturn("secret-username");
        when(context.getSecret("password")).thenReturn("secret-password");

        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(path, context);
        assertNotNull(config);
        assertEquals(config.getInfluxdbUrl(), "http://localhost:8086");
        assertEquals(config.getDatabase(), "test_db");
        assertEquals(config.getConsistencyLevel(), "ONE");
        assertEquals(config.getLogLevel(), "NONE");
        assertEquals(config.getRetentionPolicy(), "autogen");
        assertFalse(config.isGzipEnable());
        assertEquals(config.getBatchTimeMs(), Long.parseLong("1000"));
        assertEquals(config.getBatchSize(), Integer.parseInt("100"));
        assertEquals(config.getUsername(), "secret-username");
        assertEquals(config.getPassword(), "secret-password");
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("influxdbUrl", "http://localhost:8086");
        map.put("database", "test_db");
        map.put("consistencyLevel", "ONE");
        map.put("logLevel", "NONE");
        map.put("retentionPolicy", "autogen");
        map.put("gzipEnable", "false");
        map.put("batchTimeMs", "1000");
        map.put("batchSize", "100");

        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map);
        assertNotNull(config);
        assertEquals("http://localhost:8086", config.getInfluxdbUrl());
        assertEquals("test_db", config.getDatabase());
        assertEquals("ONE", config.getConsistencyLevel());
        assertEquals("NONE", config.getLogLevel());
        assertEquals("autogen", config.getRetentionPolicy());
        assertEquals(Boolean.parseBoolean("false"), config.isGzipEnable());
        assertEquals(Long.parseLong("1000"), config.getBatchTimeMs());
        assertEquals(Integer.parseInt("100"), config.getBatchSize());
    }

    @Test
    public final void loadFromMapAndContextTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("influxdbUrl", "http://localhost:8086");
        map.put("database", "test_db");
        map.put("consistencyLevel", "ONE");
        map.put("logLevel", "NONE");
        map.put("retentionPolicy", "autogen");
        map.put("gzipEnable", "false");
        map.put("batchTimeMs", "1000");
        map.put("batchSize", "100");

        SinkContext context = mock(SinkContext.class);
        when(context.getSecret("username")).thenReturn("secret-username");
        when(context.getSecret("password")).thenReturn("secret-password");

        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map, context);
        assertNotNull(config);
        assertEquals(config.getInfluxdbUrl(), "http://localhost:8086");
        assertEquals(config.getDatabase(), "test_db");
        assertEquals(config.getConsistencyLevel(), "ONE");
        assertEquals(config.getLogLevel(), "NONE");
        assertEquals(config.getRetentionPolicy(), "autogen");
        assertFalse(config.isGzipEnable());
        assertEquals(config.getBatchTimeMs(), Long.parseLong("1000"));
        assertEquals(config.getBatchSize(), Integer.parseInt("100"));
        assertEquals(config.getUsername(), "secret-username");
        assertEquals(config.getPassword(), "secret-password");
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("influxdbUrl", "http://localhost:8086");
        map.put("database", "test_db");
        map.put("consistencyLevel", "ONE");
        map.put("logLevel", "NONE");
        map.put("retentionPolicy", "autogen");
        map.put("gzipEnable", "false");
        map.put("batchTimeMs", "1000");
        map.put("batchSize", "100");

        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "influxdbUrl cannot be null")
    public final void missingInfluxdbUrlTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("database", "test_db");
        map.put("consistencyLevel", "ONE");
        map.put("logLevel", "NONE");
        map.put("retentionPolicy", "autogen");
        map.put("gzipEnable", "false");
        map.put("batchTimeMs", "1000");
        map.put("batchSize", "100");

        InfluxDBSinkConfig.load(map, null);
    }

    @Test(expectedExceptions = NullPointerException.class,
        expectedExceptionsMessageRegExp = "influxdbUrl property not set.")
    public final void missingInfluxdbUrlValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("database", "test_db");
        map.put("consistencyLevel", "ONE");
        map.put("logLevel", "NONE");
        map.put("retentionPolicy", "autogen");
        map.put("gzipEnable", "false");
        map.put("batchTimeMs", "1000");
        map.put("batchSize", "100");

        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "batchSize must be a positive integer.")
    public final void invalidBatchSizeTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("influxdbUrl", "http://localhost:8086");
        map.put("database", "test_db");
        map.put("consistencyLevel", "ONE");
        map.put("logLevel", "NONE");
        map.put("retentionPolicy", "autogen");
        map.put("gzipEnable", "false");
        map.put("batchTimeMs", "1000");
        map.put("batchSize", "-100");

        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map, null);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "No enum constant org.influxdb.InfluxDB.ConsistencyLevel.NOTSUPPORT")
    public final void invalidConsistencyLevelTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("influxdbUrl", "http://localhost:8086");
        map.put("database", "test_db");
        map.put("consistencyLevel", "NotSupport");
        map.put("logLevel", "NONE");
        map.put("retentionPolicy", "autogen");
        map.put("gzipEnable", "false");
        map.put("batchTimeMs", "1000");
        map.put("batchSize", "100");

        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map, null);
        config.validate();

        InfluxDB.ConsistencyLevel.valueOf(config.getConsistencyLevel().toUpperCase());
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}

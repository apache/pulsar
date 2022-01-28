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
package org.apache.pulsar.io.influxdb.v2;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class InfluxDBSinkConfigTest {

    @Test
    public final void testLoadFromYaml() throws Exception {
        File yamlFile = getFile("sinkConfig-v2.yaml");
        String path = yamlFile.getAbsolutePath();
        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(path);

        assertNotNull(config);
        config.validate();
        verifyValues(config);
    }

    private Map<String, Object> buildValidConfigMap() {
        Map<String, Object> map = new HashMap();
        map.put("influxdbUrl", "http://localhost:9999");
        map.put("token", "xxxx");
        map.put("organization", "example-org");
        map.put("bucket", "example-bucket");
        map.put("precision", "ns");
        map.put("logLevel", "NONE");

        map.put("gzipEnable", false);
        map.put("batchTimeMs", 1000);
        map.put("batchSize", 5000);
        return map;
    }

    @Test
    public final void testLoadFromMap() throws Exception {
        Map<String, Object> map = buildValidConfigMap();

        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map);
        assertNotNull(config);
        config.validate();
        verifyValues(config);
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "influxdbUrl property not set.")
    public void testRequiredConfigMissing() throws Exception {
        Map<String, Object> map = buildValidConfigMap();
        map.remove("influxdbUrl");
        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchSize must be a positive integer.")
    public void testBatchConfig() throws Exception {
        Map<String, Object> map = buildValidConfigMap();
        map.put("batchSize", -1);
        InfluxDBSinkConfig config = InfluxDBSinkConfig.load(map);
        config.validate();
    }

    private void verifyValues(InfluxDBSinkConfig config) {
        assertEquals("http://localhost:9999", config.getInfluxdbUrl());
        assertEquals("xxxx", config.getToken());
        assertEquals("example-org", config.getOrganization());
        assertEquals("example-bucket", config.getBucket());
        assertEquals("ns", config.getPrecision());
        assertEquals("NONE", config.getLogLevel());
        assertFalse(config.isGzipEnable());
        assertEquals(5000, config.getBatchSize());
        assertEquals(1000, config.getBatchTimeMs());
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
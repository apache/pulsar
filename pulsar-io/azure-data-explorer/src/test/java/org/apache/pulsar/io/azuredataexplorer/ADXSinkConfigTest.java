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
package org.apache.pulsar.io.azuredataexplorer;

import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mockito;

import static org.testng.Assert.*;

public class ADXSinkConfigTest {
    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File sinkConfig = readSinkConfig("sinkConfig.yaml");
        String path = sinkConfig.getAbsolutePath();
        ADXSinkConfig config = ADXSinkConfig.load(path);
        assertNotNull(config);
        assertEquals(config.getClusterUrl(), "https://somecluster.eastus.kusto.windows.net");
        assertEquals(config.getDatabase(), "somedb");
        assertEquals(config.getTable(), "tableName");
        assertEquals(config.getAppId(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getAppKey(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getTenantId(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getManagedIdentityId(), "xxxx-some-id-xxxx OR empty string");
        assertEquals(config.getMappingRefName(), "mapping ref name");
        assertEquals(config.getMappingRefType(), "CSV");
        assertFalse(config.isFlushImmediately());
        assertEquals(config.getBatchSize(), 100);
        assertEquals(config.getBatchTimeMs(), 10000);
    }

    @Test
    public final void validateConfigTest() throws Exception {
        File yamlFile = readSinkConfig("sinkConfigValid.yaml");
        String path = yamlFile.getAbsolutePath();
        ADXSinkConfig config = ADXSinkConfig.load(path);
        config.validate();
    }

    @Test(expectedExceptions = Exception.class)
    public final void validateInvalidConfigTest() throws Exception {
        File yamlFile = readSinkConfig("sinkConfigInvalid.yaml");
        String path = yamlFile.getAbsolutePath();
        ADXSinkConfig config = ADXSinkConfig.load(path);
        config.validate();
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = getConfig();
        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        ADXSinkConfig config = ADXSinkConfig.load(map, sinkContext);
        assertNotNull(config);
        assertEquals(config.getClusterUrl(), "https://somecluster.eastus.kusto.windows.net");
        assertEquals(config.getDatabase(), "somedb");
        assertEquals(config.getTable(), "tableName");
        assertEquals(config.getAppId(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getAppKey(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getTenantId(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getManagedIdentityId(), "xxxx-some-id-xxxx OR empty string");
        assertEquals(config.getMappingRefName(), "mapping ref name");
        assertEquals(config.getMappingRefType(), "CSV");
        assertFalse(config.isFlushImmediately());
        assertEquals(config.getBatchSize(), 100);
        assertEquals(config.getBatchTimeMs(), 10000);
    }

    @NotNull
    private static Map<String, Object> getConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("clusterUrl", "https://somecluster.eastus.kusto.windows.net");
        map.put("database", "somedb");
        map.put("table", "tableName");
        map.put("appId", "xxxx-xxxx-xxxx-xxxx");
        map.put("appKey", "xxxx-xxxx-xxxx-xxxx");
        map.put("tenantId", "xxxx-xxxx-xxxx-xxxx");
        map.put("managedIdentityId", "xxxx-some-id-xxxx OR empty string");
        map.put("mappingRefName", "mapping ref name");
        map.put("mappingRefType", "CSV");
        map.put("flushImmediately", false);
        map.put("batchSize", 100);
        map.put("batchTimeMs", 10000);
        return map;
    }

    private @NotNull File readSinkConfig(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(Objects.requireNonNull(classLoader.getResource(name)).getFile());
    }
}

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
package org.apache.pulsar.io.nifi;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * nifi Config test
 */
public class NiFiConfigTest {
    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("niFiConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        NiFiConfig config = NiFiConfig.load(path);
        assertNotNull(config);
        assertEquals("http://localhost:8080/nifi", config.getUrl());
        assertEquals("Data for Pulsar", config.getPortName());
        assertEquals(5, config.getRequestBatchCount());
        assertEquals(1000, config.getWaitTimeMs());
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", "http://localhost:8080/nifi");
        map.put("portName", "Data for Pulsar");
        map.put("requestBatchCount", 5);
        map.put("waitTimeMs", 1000);

        NiFiConfig config = NiFiConfig.load(map);
        assertNotNull(config);
        assertEquals("http://localhost:8080/nifi", config.getUrl());
        assertEquals("Data for Pulsar", config.getPortName());
        assertEquals(5, config.getRequestBatchCount());
        assertEquals(1000, config.getWaitTimeMs());

    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("url", "http://localhost:8080/nifi");
        map.put("portName", "Data for Pulsar");

        NiFiConfig config = NiFiConfig.load(map);
        config.validate();
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}

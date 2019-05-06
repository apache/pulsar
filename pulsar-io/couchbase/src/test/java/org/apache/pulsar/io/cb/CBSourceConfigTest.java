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
package org.apache.pulsar.io.cb;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNotNull;

@Slf4j
public class CBSourceConfigTest {
    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("cbConfig.yaml");

        CBSourceConfig config = CBSourceConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<>();

        map.put("connectionName", "apache");
        map.put("connectionTimeOut", 60);

        String[] hostnames = {"localhost"};
        map.put("hostnames", hostnames);

        map.put("bucket", "travel-sample");
        map.put("username", "Administrator");
        map.put("password", "password");
        map.put("compressedMode", "ENABLED");
        map.put("persistencePollingInterval", 6000);
        map.put("flowControlBufferSizeInBytes", 10240);

        map.put("sslEnabled", false);
        map.put("sslKeystoreFile", "");
        map.put("sslKeystorePassword", "");

        CBSourceConfig config = CBSourceConfig.load(map);
        assertNotNull(config);
    }

    private File getFile(String name) {
        return new File(getClass().getClassLoader().getResource(name).getFile());
    }
}

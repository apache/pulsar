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
package org.apache.pulsar.io.alluxio.sink;

import alluxio.client.WriteType;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * AlluxioSinkConfig test
 */
public class AlluxioSinkConfigTest {

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        AlluxioSinkConfig config = AlluxioSinkConfig.load(path);
        assertNotNull(config);
        assertEquals("localhost", config.getAlluxioMasterHost());
        assertEquals(Integer.parseInt("19998"), config.getAlluxioMasterPort());
        assertEquals("pulsar", config.getAlluxioDir());
        assertEquals("TopicA", config.getFilePrefix());
        assertEquals(".txt", config.getFileExtension());
        assertEquals("\n".charAt(0), config.getLineSeparator());
        assertEquals(Long.parseLong("100"), config.getRotationRecords());
        assertEquals(Long.parseLong("-1"), config.getRotationInterval());
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("alluxioMasterHost", "localhost");
        map.put("alluxioMasterPort", "19998");
        map.put("alluxioDir", "pulsar");
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "100");
        map.put("rotationInterval", "-1");

        AlluxioSinkConfig config = AlluxioSinkConfig.load(map);
        assertNotNull(config);
        assertEquals("localhost", config.getAlluxioMasterHost());
        assertEquals(Integer.parseInt("19998"), config.getAlluxioMasterPort());
        assertEquals("pulsar", config.getAlluxioDir());
        assertEquals("TopicA", config.getFilePrefix());
        assertEquals(".txt", config.getFileExtension());
        assertEquals("\n".charAt(0), config.getLineSeparator());
        assertEquals(Long.parseLong("100"), config.getRotationRecords());
        assertEquals(Long.parseLong("-1"), config.getRotationInterval());
    }

    @Test
    public final void validateTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("alluxioMasterHost", "localhost");
        map.put("alluxioMasterPort", "19998");
        map.put("alluxioDir", "pulsar");
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "100");
        map.put("rotationInterval", "-1");

        AlluxioSinkConfig config = AlluxioSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = NullPointerException.class,
        expectedExceptionsMessageRegExp = "alluxioDir property not set.")
    public final void missingValidateAlluxioDirTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("alluxioMasterHost", "localhost");
        map.put("alluxioMasterPort", "19998");
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "100");
        map.put("rotationInterval", "-1");

        AlluxioSinkConfig config = AlluxioSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "rotationRecords must be a positive long.")
    public final void invalidRotationRecordsTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("alluxioMasterHost", "localhost");
        map.put("alluxioMasterPort", "19998");
        map.put("alluxioDir", "pulsar");
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "-100");
        map.put("rotationInterval", "-1");

        AlluxioSinkConfig config = AlluxioSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "rotationInterval must be either -1 or a positive long.")
    public final void invalidRotationIntervalTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("alluxioMasterHost", "localhost");
        map.put("alluxioMasterPort", "19998");
        map.put("alluxioDir", "pulsar");
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "100");
        map.put("rotationInterval", "-1000");

        AlluxioSinkConfig config = AlluxioSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "No enum constant alluxio.client.WriteType.NOTSUPPORT")
    public final void invalidClientModeTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("alluxioMasterHost", "localhost");
        map.put("alluxioMasterPort", "19998");
        map.put("alluxioDir", "pulsar");
        map.put("filePrefix", "TopicA");
        map.put("fileExtension", ".txt");
        map.put("lineSeparator", "\n");
        map.put("rotationRecords", "100");
        map.put("rotationInterval", "-1");
        map.put("writeType", "NotSupport");

        AlluxioSinkConfig config = AlluxioSinkConfig.load(map);
        config.validate();

        WriteType.valueOf(config.getWriteType().toUpperCase());
    }


    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}

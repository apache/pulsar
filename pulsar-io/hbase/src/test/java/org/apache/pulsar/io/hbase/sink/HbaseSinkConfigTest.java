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
package org.apache.pulsar.io.hbase.sink;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * HbaseSinkConfig test
 */
public class HbaseSinkConfigTest {

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        HbaseSinkConfig config = HbaseSinkConfig.load(path);
        assertNotNull(config);
        assertEquals("hbase-site.xml", config.getHbaseConfigResources());
        assertEquals("localhost", config.getZookeeperQuorum());
        assertEquals("2181", config.getZookeeperClientPort());
        assertEquals("/hbase", config.getZookeeperZnodeParent());
        assertEquals("pulsar_hbase", config.getTableName());
        assertEquals("rowKey", config.getRowKeyName());
        assertEquals("info", config.getFamilyName());

        List<String> qualifierNames = new ArrayList<>();
        qualifierNames.add("name");
        qualifierNames.add("address");
        qualifierNames.add("age");
        assertEquals(qualifierNames, config.getQualifierNames());
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("hbaseConfigResources", "hbase-site.xml");
        map.put("zookeeperQuorum", "localhost");
        map.put("zookeeperClientPort", "2181");
        map.put("zookeeperZnodeParent", "/hbase");
        map.put("tableName", "pulsar_hbase");
        map.put("rowKeyName", "rowKey");
        map.put("familyName", "info");

        HbaseSinkConfig config = HbaseSinkConfig.load(map);
        assertNotNull(config);
        assertEquals("hbase-site.xml", config.getHbaseConfigResources());
        assertEquals("localhost", config.getZookeeperQuorum());
        assertEquals("2181", config.getZookeeperClientPort());
        assertEquals("/hbase", config.getZookeeperZnodeParent());
        assertEquals("pulsar_hbase", config.getTableName());
        assertEquals("rowKey", config.getRowKeyName());
        assertEquals("info", config.getFamilyName());

    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("zookeeperQuorum", "localhost");
        map.put("zookeeperClientPort", "2181");
        map.put("zookeeperZnodeParent", "/hbase");
        map.put("tableName", "pulsar_hbase");
        map.put("rowKeyName", "rowKey");
        map.put("familyName", "info");
        List<String> qualifierNames = new ArrayList<>();
        qualifierNames.add("qualifierName");
        map.put("qualifierNames", qualifierNames);

        HbaseSinkConfig config = HbaseSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = NullPointerException.class,
            expectedExceptionsMessageRegExp = "hbase tableName property not set.")
    public final void missingValidValidateTableNameTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("zookeeperQuorum", "localhost");
        map.put("zookeeperClientPort", "2181");
        map.put("zookeeperZnodeParent", "/hbase");
        map.put("rowKeyName", "rowKey");
        map.put("familyName", "info");
        List<String> qualifierNames = new ArrayList<>();
        qualifierNames.add("qualifierName");
        map.put("qualifierNames", qualifierNames);

        HbaseSinkConfig config = HbaseSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = MismatchedInputException.class)
    public final void invalidListValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("zookeeperQuorum", "localhost");
        map.put("zookeeperClientPort", "2181");
        map.put("zookeeperZnodeParent", "/hbase");
        map.put("tableName", "pulsar_hbase");
        map.put("rowKeyName", "rowKey");
        map.put("familyName", "info");
        map.put("qualifierNames", new ArrayList<>().add("name"));

        HbaseSinkConfig config = HbaseSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchTimeMs must be a positive long.")
    public final void invalidBatchTimeMsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("zookeeperQuorum", "localhost");
        map.put("zookeeperClientPort", "2181");
        map.put("zookeeperZnodeParent", "/hbase");
        map.put("tableName", "pulsar_hbase");
        map.put("rowKeyName", "rowKey");
        map.put("familyName", "info");
        List<String> qualifierNames = new ArrayList<>();
        qualifierNames.add("qualifierName");
        map.put("qualifierNames", qualifierNames);
        map.put("batchTimeMs",-10);

        HbaseSinkConfig config = HbaseSinkConfig.load(map);
        config.validate();
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}

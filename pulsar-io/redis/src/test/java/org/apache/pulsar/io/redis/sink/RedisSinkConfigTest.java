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
package org.apache.pulsar.io.redis.sink;

import org.apache.pulsar.io.redis.RedisAbstractConfig;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * RedisSinkConfig test
 */
public class RedisSinkConfigTest {

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        RedisSinkConfig config = RedisSinkConfig.load(path);
        assertNotNull(config);
        assertEquals(config.getRedisHosts(), "localhost:6379");
        assertEquals(config.getRedisPassword(), "fake@123");
        assertEquals(config.getRedisDatabase(), Integer.parseInt("1"));
        assertEquals(config.getClientMode(), "Standalone");
        assertEquals(config.getOperationTimeout(), Long.parseLong("2000"));
        assertEquals(config.getBatchSize(), Integer.parseInt("100"));
        assertEquals(config.getBatchTimeMs(), Long.parseLong("1000"));
        assertEquals(config.getConnectTimeout(), Long.parseLong("3000"));
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("redisHosts", "localhost:6379");
        map.put("redisPassword", "fake@123");
        map.put("redisDatabase", "1");
        map.put("clientMode", "Standalone");
        map.put("operationTimeout", "2000");
        map.put("batchSize", "100");
        map.put("batchTimeMs", "1000");
        map.put("connectTimeout", "3000");

        RedisSinkConfig config = RedisSinkConfig.load(map);
        assertNotNull(config);
        assertEquals(config.getRedisHosts(), "localhost:6379");
        assertEquals(config.getRedisPassword(), "fake@123");
        assertEquals(config.getRedisDatabase(), Integer.parseInt("1"));
        assertEquals(config.getClientMode(), "Standalone");
        assertEquals(config.getOperationTimeout(), Long.parseLong("2000"));
        assertEquals(config.getBatchSize(), Integer.parseInt("100"));
        assertEquals(config.getBatchTimeMs(), Long.parseLong("1000"));
        assertEquals(config.getConnectTimeout(), Long.parseLong("3000"));
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("redisHosts", "localhost:6379");
        map.put("redisPassword", "fake@123");
        map.put("redisDatabase", "1");
        map.put("clientMode", "Standalone");
        map.put("operationTimeout", "2000");
        map.put("batchSize", "100");
        map.put("batchTimeMs", "1000");
        map.put("connectTimeout", "3000");

        RedisSinkConfig config = RedisSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = NullPointerException.class,
        expectedExceptionsMessageRegExp = "redisHosts property not set.")
    public final void missingValidValidateTableNameTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("redisPassword", "fake@123");
        map.put("redisDatabase", "1");
        map.put("clientMode", "Standalone");
        map.put("operationTimeout", "2000");
        map.put("batchSize", "100");
        map.put("batchTimeMs", "1000");
        map.put("connectTimeout", "3000");

        RedisSinkConfig config = RedisSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "batchTimeMs must be a positive long.")
    public final void invalidBatchTimeMsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("redisHosts", "localhost:6379");
        map.put("redisPassword", "fake@123");
        map.put("redisDatabase", "1");
        map.put("clientMode", "Standalone");
        map.put("operationTimeout", "2000");
        map.put("batchSize", "100");
        map.put("batchTimeMs", "-100");
        map.put("connectTimeout", "3000");

        RedisSinkConfig config = RedisSinkConfig.load(map);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "No enum constant org.apache.pulsar.io.redis.RedisAbstractConfig.ClientMode.NOTSUPPORT")
    public final void invalidClientModeTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("redisHosts", "localhost:6379");
        map.put("redisPassword", "fake@123");
        map.put("redisDatabase", "1");
        map.put("clientMode", "NotSupport");
        map.put("operationTimeout", "2000");
        map.put("batchSize", "100");
        map.put("batchTimeMs", "1000");
        map.put("connectTimeout", "3000");

        RedisSinkConfig config = RedisSinkConfig.load(map);
        config.validate();

        RedisAbstractConfig.ClientMode.valueOf(config.getClientMode().toUpperCase());
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}

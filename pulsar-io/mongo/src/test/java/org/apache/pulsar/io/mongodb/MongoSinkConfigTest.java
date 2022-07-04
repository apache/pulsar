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

package org.apache.pulsar.io.mongodb;

import java.util.Map;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class MongoSinkConfigTest {

    @Test
    public void testLoadMapConfig() throws IOException {
        final Map<String, Object> commonConfigMap = TestHelper.createCommonConfigMap();
        commonConfigMap.put("batchSize", TestHelper.BATCH_SIZE);
        commonConfigMap.put("batchTimeMs", TestHelper.BATCH_TIME);

        final MongoSinkConfig cfg = MongoSinkConfig.load(commonConfigMap);

        assertEquals(cfg.getMongoUri(), TestHelper.URI);
        assertEquals(cfg.getDatabase(), TestHelper.DB);
        assertEquals(cfg.getCollection(), TestHelper.COLL);
        assertEquals(cfg.getBatchSize(), TestHelper.BATCH_SIZE);
        assertEquals(cfg.getBatchTimeMs(), TestHelper.BATCH_TIME);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Required MongoDB URI is not set.")
    public void testBadMongoUri() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.removeMongoUri(configMap);

        final MongoSinkConfig cfg = MongoSinkConfig.load(configMap);

        cfg.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Required MongoDB database name is not set.")
    public void testBadDatabase() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.removeDatabase(configMap);

        final MongoSinkConfig cfg = MongoSinkConfig.load(configMap);

        cfg.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Required MongoDB collection name is not set.")
    public void testBadCollection() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.removeCollection(configMap);

        final MongoSinkConfig cfg = MongoSinkConfig.load(configMap);

        cfg.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchSize must be a positive integer.")
    public void testBadBatchSize() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.putBatchSize(configMap, 0);

        final MongoSinkConfig cfg = MongoSinkConfig.load(configMap);

        cfg.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchTimeMs must be a positive long.")
    public void testBadBatchTime() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.putBatchTime(configMap, 0L);

        final MongoSinkConfig cfg = MongoSinkConfig.load(configMap);

        cfg.validate();
    }

    @Test
    public void testLoadYamlConfig() throws IOException {
        final File yaml = TestHelper.getFile(MongoSinkConfigTest.class, "mongoSinkConfig.yaml");
        final MongoSinkConfig cfg = MongoSinkConfig.load(yaml.getAbsolutePath());

        assertEquals(cfg.getMongoUri(), TestHelper.URI);
        assertEquals(cfg.getDatabase(), TestHelper.DB);
        assertEquals(cfg.getCollection(), TestHelper.COLL);
        assertEquals(cfg.getBatchSize(), TestHelper.BATCH_SIZE);
        assertEquals(cfg.getBatchTimeMs(), TestHelper.BATCH_TIME);
    }
}

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

import static org.testng.Assert.assertEquals;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.testng.annotations.Test;

public class MongoSourceConfigTest {

    @Test
    public void testLoadMapConfig() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.putSyncType(configMap, TestHelper.SYNC_TYPE);

        final MongoSourceConfig cfg = MongoSourceConfig.load(configMap);

        assertEquals(cfg.getMongoUri(), TestHelper.URI);
        assertEquals(cfg.getDatabase(), TestHelper.DB);
        assertEquals(cfg.getCollection(), TestHelper.COLL);
        assertEquals(cfg.getSyncType(), TestHelper.SYNC_TYPE);
        assertEquals(cfg.getBatchSize(), TestHelper.BATCH_SIZE);
        assertEquals(cfg.getBatchTimeMs(), TestHelper.BATCH_TIME);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Required MongoDB URI is not set.")
    public void testBadMongoUri() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.removeMongoUri(configMap);

        final MongoSourceConfig cfg = MongoSourceConfig.load(configMap);

        cfg.validate();
    }

    /**
     * Test whether an exception is thrown when the syncType field has an incorrect value.
     */
    @Test(expectedExceptions = {IllegalArgumentException.class, JsonMappingException.class})
    public void testBadSyncType() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.putSyncType(configMap, "wrong_sync_type_str");

        final MongoSourceConfig cfg = MongoSourceConfig.load(configMap);

        cfg.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchSize must be a positive integer.")
    public void testBadBatchSize() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.putBatchSize(configMap, 0);

        final MongoSourceConfig cfg = MongoSourceConfig.load(configMap);

        cfg.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchTimeMs must be a positive long.")
    public void testBadBatchTime() throws IOException {
        final Map<String, Object> configMap = TestHelper.createCommonConfigMap();
        TestHelper.putBatchTime(configMap, 0L);

        final MongoSourceConfig cfg = MongoSourceConfig.load(configMap);

        cfg.validate();
    }

    @Test
    public void testLoadYamlConfig() throws IOException {
        final File yaml = TestHelper.getFile(MongoSourceConfigTest.class, "mongoSourceConfig.yaml");
        final MongoSourceConfig cfg = MongoSourceConfig.load(yaml.getAbsolutePath());

        assertEquals(cfg.getMongoUri(), TestHelper.URI);
        assertEquals(cfg.getDatabase(), TestHelper.DB);
        assertEquals(cfg.getCollection(), TestHelper.COLL);
        assertEquals(cfg.getSyncType(), TestHelper.SYNC_TYPE);
        assertEquals(cfg.getBatchSize(), TestHelper.BATCH_SIZE);
        assertEquals(cfg.getBatchTimeMs(), TestHelper.BATCH_TIME);
    }
}

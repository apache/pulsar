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

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class MongoConfigTest {

    private static File getFile(String fileName) {
        return new File(MongoConfigTest.class.getClassLoader().getResource(fileName).getFile());
    }

    @Test
    public void testMap() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(true);
        final MongoConfig cfg = MongoConfig.load(map);

        assertEquals(cfg.getMongoUri(), TestHelper.URI);
        assertEquals(cfg.getDatabase(), TestHelper.DB);
        assertEquals(cfg.getCollection(), TestHelper.COLL);
        assertEquals(cfg.getBatchSize(), TestHelper.BATCH_SIZE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Required property not set.")
    public void testBadMap() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(false);
        final MongoConfig cfg = MongoConfig.load(map);

        cfg.validate(true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchSize must be a positive integer.")
    public void testBadBatchSize() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(true);
        map.put("batchSize", 0);
        final MongoConfig cfg = MongoConfig.load(map);

        cfg.validate(true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "batchTimeMs must be a positive long.")
    public void testBadBatchTime() throws IOException {
        final Map<String, Object> map = TestHelper.createMap(true);
        map.put("batchTimeMs", 0);
        final MongoConfig cfg = MongoConfig.load(map);

        cfg.validate(true, true);
    }

    @Test
    public void testYaml() throws IOException {
        final File yaml = getFile("mongoSinkConfig.yaml");
        final MongoConfig cfg = MongoConfig.load(yaml.getAbsolutePath());

        assertEquals(cfg.getMongoUri(), TestHelper.URI);
        assertEquals(cfg.getDatabase(), TestHelper.DB);
        assertEquals(cfg.getCollection(), TestHelper.COLL);
        assertEquals(cfg.getBatchSize(), TestHelper.BATCH_SIZE);
        assertEquals(cfg.getBatchTimeMs(), TestHelper.BATCH_TIME);
    }
}

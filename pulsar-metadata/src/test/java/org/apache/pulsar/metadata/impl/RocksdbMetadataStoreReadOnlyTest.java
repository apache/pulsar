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

package org.apache.pulsar.metadata.impl;

import static org.testng.Assert.assertEquals;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class RocksdbMetadataStoreReadOnlyTest {

    Path readonlyDataDir;

    @BeforeClass
    public void initData() throws Exception {
        readonlyDataDir = Files.createTempDirectory("RocksDBMetadataReadOnlyTest");

        MetadataStore store = MetadataStoreFactory.create("rocksdb:"+readonlyDataDir, MetadataStoreConfig.builder().build());
         // put init data to rocksdb
        store.put("/test","hello".getBytes(), Optional.of(-1L)).get();
        store.close();
    }

    @AfterClass
    public void deleteTmpData(){
        FileUtils.deleteQuietly(readonlyDataDir.toFile());
    }

    @BeforeMethod
    public void setReadOnlyToFalse() throws Exception {
        System.setProperty("pulsar.metadata.rocksdb.readonly","false");
    }

    public void setReadOnlyToTrue() {
        System.setProperty("pulsar.metadata.rocksdb.readonly","true");
    }

    @Test
    public void putFailedWhenReadOnly() throws Exception {
        setReadOnlyToTrue();
        MetadataStore store = MetadataStoreFactory.create("rocksdb:"+readonlyDataDir, MetadataStoreConfig.builder().build());
        try {
            store.put("/test", "hello".getBytes(), Optional.of(0L)).get();
        } catch (Exception e) {
            MetadataStoreException e1 = (MetadataStoreException)e.getCause();
            assertEquals(e1.getMessage(),"ReadOnly mode not support put operation.");
        }
        deleteTmpData();
    }

    @Test
    public void deleteFailedWhenReadOnly() throws Exception {
        setReadOnlyToTrue();
        MetadataStore store = MetadataStoreFactory.create("rocksdb:"+readonlyDataDir, MetadataStoreConfig.builder().build());
        try {
            store.delete("/test",Optional.empty()).get();
        } catch (Exception e) {
            MetadataStoreException e1 = (MetadataStoreException)e.getCause();
            assertEquals(e1.getMessage(),"ReadOnly mode not support delete operation.");
        }
        deleteTmpData();
    }

}
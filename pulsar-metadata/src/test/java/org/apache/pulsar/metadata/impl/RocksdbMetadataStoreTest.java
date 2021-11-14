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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class RocksdbMetadataStoreTest {

    @Test
    public void testConvert() {
        String s = "testConvert";
        Assert.assertEquals(s, RocksdbMetadataStore.toString(RocksdbMetadataStore.toBytes(s)));

        long l = 12345;
        Assert.assertEquals(l, RocksdbMetadataStore.toLong(RocksdbMetadataStore.toBytes(l)));
    }

    @Test
    public void testMetaValue() throws Exception {
        RocksdbMetadataStore.MetaValue metaValue = new RocksdbMetadataStore.MetaValue();
        metaValue.setVersion(RandomUtils.nextLong());
        metaValue.setOwner(RandomUtils.nextLong());
        metaValue.setCreatedTimestamp(RandomUtils.nextLong());
        metaValue.setModifiedTimestamp(RandomUtils.nextLong());
        metaValue.setEphemeral(RandomUtils.nextBoolean());
        metaValue.setData(String.valueOf(RandomUtils.nextDouble()).getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(RocksdbMetadataStore.MetaValue.parse(metaValue.serialize()), metaValue);
    }

    @Test
    public void testOpenDbWithConfigFile() throws Exception {
        MetadataStore store;
        Path tempDir;
        tempDir = Files.createTempDirectory("RocksdbMetadataStoreTest");
        log.info("Temp dir:{}", tempDir.toAbsolutePath());
        String optionFilePath =
                getClass().getClassLoader().getResource("rocksdb_option_file_example.ini").getPath();
        log.info("optionFilePath={}", optionFilePath);
        store = MetadataStoreFactory.create("rocksdb://" + tempDir.toAbsolutePath(),
                MetadataStoreConfig.builder().configFilePath(optionFilePath).build());
        Assert.assertTrue(store instanceof RocksdbMetadataStore);

        String path = "/test";
        byte[] data = "data".getBytes();

        //test put
        CompletableFuture<Stat> f = store.put(path, data, Optional.of(-1L));

        CompletableFuture<Stat> failedPut = store.put(path, data, Optional.of(100L));
        Assert.expectThrows(MetadataStoreException.BadVersionException.class, () -> {
            try {
                failedPut.get();
            } catch (ExecutionException t) {
                throw t.getCause();
            }
        });

        Assert.assertNotNull(f.get());
        log.info("put result:{}", f.get());
        Assert.assertNotNull(store.put(path + "/a", data, Optional.of(-1L)));
        Assert.assertNotNull(store.put(path + "/b", data, Optional.of(-1L)));
        Assert.assertNotNull(store.put(path + "/c", data, Optional.of(-1L)));

        //reopen db
        store.close();
        store = MetadataStoreFactory.create("rocksdb://" + tempDir.toAbsolutePath(),
                MetadataStoreConfig.builder().configFilePath(optionFilePath).build());

        //test get
        CompletableFuture<Optional<GetResult>> readResult = store.get(path);
        Assert.assertNotNull(readResult.get());
        Assert.assertTrue(readResult.get().isPresent());
        GetResult r = readResult.get().get();
        Assert.assertEquals(path, r.getStat().getPath());
        Assert.assertEquals(0, r.getStat().getVersion());
        Assert.assertEquals(data, r.getValue());

        store.close();
        FileUtils.deleteQuietly(tempDir.toFile());
    }
}
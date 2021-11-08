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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class RocksdbMetadataStoreTest {

    private static MetadataStore store;
    private static Path tempDir;


    @BeforeClass
    public static void beforeClass() throws Exception {
        tempDir = Files.createTempDirectory("RocksdbMetadataStoreTest");
        log.info("Temp dir:{}", tempDir.toAbsolutePath());
        store = MetadataStoreFactory.create("rocksdb://" + tempDir.toAbsolutePath(),
                MetadataStoreConfig.builder().build());
        Assert.assertTrue(store instanceof RocksdbMetadataStore);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        store.close();
        FileUtils.deleteQuietly(tempDir.toFile());
    }

    @Test
    public void testConvert() {
        String s = "testConvert";
        Assert.assertEquals(s, RocksdbMetadataStore.toString(RocksdbMetadataStore.toBytes(s)));

        long l = 12345;
        Assert.assertEquals(l, RocksdbMetadataStore.toLong(RocksdbMetadataStore.toBytes(l)));
    }

    @Test
    public void testMetadataStore() throws Exception {
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

        //test get
        CompletableFuture<Optional<GetResult>> readResult = store.get(path);
        Assert.assertNotNull(readResult.get());
        Assert.assertTrue(readResult.get().isPresent());
        GetResult r = readResult.get().get();
        Assert.assertEquals(path, r.getStat().getPath());
        Assert.assertEquals(0, r.getStat().getVersion());
        Assert.assertEquals(data, r.getValue());

        //test get children
        CompletableFuture<List<String>> childResult = store.getChildren(path);
        Assert.assertNotNull(childResult.get());
        Assert.assertEquals(Lists.newArrayList("a", "b", "c"), childResult.get());

        //test delete
        CompletableFuture<Void> failDeleteFuture = store.delete(path, Optional.of(r.getStat().getVersion() + 1));
        Assert.expectThrows(MetadataStoreException.BadVersionException.class, () -> {
            try {
                failDeleteFuture.get();
            } catch (ExecutionException t) {
                throw t.getCause();
            }
        });

        CompletableFuture<Void> deleteFuture;
        deleteFuture = store.delete(path, Optional.empty());
        Assert.assertNull(deleteFuture.get());
        Assert.assertFalse(deleteFuture.isCompletedExceptionally());

        //test exists
        Assert.assertFalse(store.exists(path).get());
    }

    @Test
    public void testSequential() throws Exception {
        Assert.assertTrue(store instanceof MetadataStoreExtended);
        MetadataStoreExtended extendedStore = (MetadataStoreExtended) store;
        String path = "/test/Sequential";
        byte[] data = "data".getBytes();
        CompletableFuture<Stat> putFuture = extendedStore.put(path, data, Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral, CreateOption.Sequential));
        Assert.assertNotNull(putFuture.get());
        String path1 = putFuture.get().getPath();
        log.info("testSequential, path1:{}", path1);
        long id1 = Long.parseLong(path1.substring(path.length()));
        log.info("testSequential, id1:{}", id1);
        Assert.assertTrue(id1 >= 0);

        String path2 = extendedStore.put(path, data, Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral, CreateOption.Sequential)).get().getPath();
        long id2 = Long.parseLong(path2.substring(path.length()));
        log.info("testSequential, path:{},id2:{}", path2, id2);
        Assert.assertTrue(id2 > id1);
    }

    @Test
    public void testNotification() throws ExecutionException, InterruptedException {
        List<Notification> notificationHolder = new CopyOnWriteArrayList<>();
        store.registerListener(notification -> {
            log.info("NOTIFY:{}", notification);
            notificationHolder.add(notification);
        });

        String path = "/test/Sequential";
        byte[] data = "data".getBytes();
        store.put(path, data, Optional.empty());
        Awaitility.await().until(()->notificationHolder.contains(new Notification(NotificationType.Created, path)));
        Awaitility.await().until(()->notificationHolder.contains(new Notification(NotificationType.ChildrenChanged, "/test")));
        notificationHolder.clear();

        store.put(path, "data1".getBytes(), Optional.empty());
        Awaitility.await().until(()->notificationHolder.contains(new Notification(NotificationType.Modified, path)));
        notificationHolder.clear();

        store.put(path + "/a", data, Optional.empty());
        Awaitility.await().until(()->notificationHolder.contains(new Notification(NotificationType.Created, path + "/a")));
        Awaitility.await().until(()->notificationHolder.contains(new Notification(NotificationType.ChildrenChanged, path)));
        notificationHolder.clear();

        store.delete(path, Optional.empty());
        Awaitility.await().until(()->notificationHolder.contains(new Notification(NotificationType.Deleted, path)));
        Awaitility.await().until(()->notificationHolder.contains(new Notification(NotificationType.ChildrenChanged, "/test")));
    }
}
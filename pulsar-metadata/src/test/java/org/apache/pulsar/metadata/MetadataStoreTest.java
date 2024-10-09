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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.session.SessionFactory;
import io.streamnative.oxia.client.session.SessionManager;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.PulsarZooKeeperClient;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.metadata.impl.oxia.OxiaMetadataStore;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class MetadataStoreTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void emptyStoreTest(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        assertFalse(store.exists(prefix + "/non-existing-key").join());
        assertFalse(store.exists(prefix + "/non-existing-key/child").join());
        assertFalse(store.get(prefix + "/non-existing-key").join().isPresent());
        assertFalse(store.get(prefix + "/non-existing-key/child").join().isPresent());

        assertEquals(store.getChildren(prefix + "/non-existing-key").join(), Collections.emptyList());
        assertEquals(store.getChildren(prefix + "/non-existing-key/child").join(), Collections.emptyList());

        try {
            store.delete(prefix + "/non-existing-key", Optional.empty()).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, NotFoundException.class);
        }

        try {
            store.delete(prefix + "/non-existing-key", Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertTrue(NotFoundException.class.isInstance(e.getCause()) || BadVersionException.class.isInstance(
                    e.getCause()));
        }
    }

    @Test(dataProvider = "impl")
    public void concurrentPutTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String data = "data";
        String path = "/non-existing-key";
        int concurrent = 50;
        List<CompletableFuture<Stat>> futureList = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            futureList.add(store.put(path, data.getBytes(), Optional.empty()).exceptionally(ex -> {
                fail("fail to execute concurrent put", ex);
                return null;
            }));
        }
        FutureUtil.waitForAll(futureList).join();

        assertEquals(store.get(path).join().get().getValue(), data.getBytes());
    }

    @Test(dataProvider = "impl")
    public void insertionTestWithExpectedVersion(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String key1 = newKey();

        try {
            store.put(key1, "value-1".getBytes(), Optional.of(0L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        try {
            store.put(key1, "value-1".getBytes(), Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        var putRes = store.put(key1, "value-1".getBytes(), Optional.of(-1L)).join();
        long putVersion = putRes.getVersion();
        assertTrue(putVersion >= 0);
        assertTrue(putRes.isFirstVersion());

        assertTrue(store.exists(key1).join());
        Optional<GetResult> optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-1".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), putVersion);

        try {
            store.put(key1, "value-2".getBytes(), Optional.of(-1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        try {
            store.put(key1, "value-2".getBytes(), Optional.of(putVersion + 1)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        assertTrue(store.exists(key1).join());
        optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-1".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), putVersion);

        putRes = store.put(key1, "value-2".getBytes(), Optional.of(putVersion)).join();
        assertTrue(putRes.getVersion() > putVersion);

        assertTrue(store.exists(key1).join());
        optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-2".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), putRes.getVersion());
    }

    @Test(dataProvider = "impl")
    public void getChildrenTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String key = newKey();
        int n = 10;
        List<String> expectedChildren = new ArrayList<>();

        assertEquals(store.getChildren(key).join(), Collections.emptyList());

        for (int i = 0; i < n; i++) {
            store.put(key + "/c-" + i, new byte[0], Optional.empty()).join();

            expectedChildren.add("c-" + i);
        }

        assertEquals(store.getChildren(key).join(), expectedChildren);

        // Nested children
        for (int i = 0; i < n; i++) {
            store.put(key + "/c-0/cc-" + i, new byte[0], Optional.empty()).join();
        }

        assertEquals(store.getChildren(key).join(), expectedChildren);

        for (int i = 0; i < n; i++) {
            store.deleteRecursive(key + "/c-" + i).join();
        }

        assertEquals(store.getChildren(key).join(), Collections.emptyList());
    }

    @Test(dataProvider = "impl")
    public void navigateChildrenTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        String key = newKey();

        // Nested children
        store.put(key + "/c-0/cc-1", new byte[0], Optional.empty()).join();
        store.put(key + "/c-0/cc-2/ccc-1", new byte[0], Optional.empty()).join();

        assertEquals(store.getChildren(key).join(), Collections.singletonList("c-0"));
        assertEquals(store.getChildren(key + "/c-0").join(),
                Lists.newArrayList("cc-1", "cc-2"));
        assertEquals(store.getChildren(key + "/c-0/cc-2").join(),
                Lists.newArrayList("ccc-1"));
    }

    @Test(dataProvider = "impl")
    public void deletionTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String key = newKey();
        int n = 10;
        List<String> expectedChildren = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            store.put(key + "/c-" + i, new byte[0], Optional.empty()).join();

            expectedChildren.add("c-" + i);
        }

        try {
            store.delete(key, Optional.empty()).join();
            fail("The key has children");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        for (int i = 0; i < n; i++) {
            try {
                store.delete(key + "/c-" + i, Optional.of(1L)).join();
                fail("The key has children");
            } catch (CompletionException e) {
                assertException(e, BadVersionException.class);
            }

            store.delete(key + "/c-" + i, Optional.empty()).join();
        }
    }

    @Test(dataProvider = "impl")
    public void emptyKeyTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        try {
            store.delete("", Optional.empty()).join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        try {
            store.getChildren("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        try {
            store.get("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        try {
            store.exists("").join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }

        try {
            store.put("", new byte[0], Optional.empty()).join();
            fail("The key cannot be empty");
        } catch (CompletionException e) {
            assertException(e, MetadataStoreException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void notificationListeners(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        BlockingQueue<Notification> notifications = new LinkedBlockingDeque<>();
        store.registerListener(n -> {
            notifications.add(n);
        });

        String key1 = newKey();

        assertFalse(store.get(key1).join().isPresent());

        // Trigger created notification
        Stat stat = store.put(key1, "value-1".getBytes(), Optional.empty()).join();
        assertTrue(store.get(key1).join().isPresent());
        assertEquals(store.getChildren(key1).join(), Collections.emptyList());
        assertTrue(stat.getVersion() >= 0);
        assertTrue(stat.isFirstVersion());

        Notification n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.Created);
        assertEquals(n.getPath(), key1);
        var firstVersion = stat.getVersion();

        // Trigger modified notification
        stat = store.put(key1, "value-2".getBytes(), Optional.empty()).join();
        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.Modified);
        assertEquals(n.getPath(), key1);
        assertTrue(stat.getVersion() > firstVersion);

        // Trigger modified notification on the parent
        String key1Child = key1 + "/xx";

        assertFalse(store.get(key1Child).join().isPresent());

        store.put(key1Child, "value-2".getBytes(), Optional.empty()).join();
        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.Created);
        assertEquals(n.getPath(), key1Child);

        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.ChildrenChanged);
        assertEquals(n.getPath(), key1);

        assertTrue(store.exists(key1Child).join());
        assertEquals(store.getChildren(key1).join(), Collections.singletonList("xx"));

        store.delete(key1Child, Optional.empty()).join();

        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.Deleted);
        assertEquals(n.getPath(), key1Child);

        // Parent should be notified of the deletion
        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.ChildrenChanged);
        assertEquals(n.getPath(), key1);
    }

    @Test(dataProvider = "impl")
    public void testDeleteRecursive(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String prefix = newKey();

        String key1 = newKey();
        store.put(prefix + key1, "value-1".getBytes(), Optional.of(-1L)).join();

        store.put(prefix + key1 + "/c1", "value".getBytes(), Optional.of(-1L)).join();
        store.put(prefix + key1 + "/c2", "value".getBytes(), Optional.of(-1L)).join();
        store.put(prefix + key1 + "/c1/x1", "value".getBytes(), Optional.of(-1L)).join();
        store.put(prefix + key1 + "/c1/x2", "value".getBytes(), Optional.of(-1L)).join();
        store.put(prefix + key1 + "/c2/y2", "value".getBytes(), Optional.of(-1L)).join();
        store.put(prefix + key1 + "/c3", "value".getBytes(), Optional.of(-1L)).join();

        String key2 = newKey();
        store.put(prefix + key2, "value-2".getBytes(), Optional.of(-1L)).join();

        store.deleteRecursive(prefix + key1).join();

        assertEquals(store.getChildren(prefix).join(), Collections.singletonList(key2.substring(1)));
    }

    @Test(dataProvider = "impl")
    public void testDeleteUnusedDirectories(String provider, Supplier<String> urlSupplier) throws Exception {
        if (provider.equals("Oxia")) {
            return;
        }

        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String prefix = newKey();

        store.put(prefix + "/a1/b1/c1", "value".getBytes(), Optional.of(-1L)).join();
        store.put(prefix + "/a1/b1/c2", "value".getBytes(), Optional.of(-1L)).join();
        store.put(prefix + "/a1/b2/c1", "value".getBytes(), Optional.of(-1L)).join();

        store.delete(prefix + "/a1/b1/c1", Optional.empty()).join();
        store.delete(prefix + "/a1/b1/c2", Optional.empty()).join();

        zks.checkContainers();
        assertFalse(store.exists(prefix + "/a1/b1").join());

        store.delete(prefix + "/a1/b2/c1", Optional.empty()).join();

        zks.checkContainers();
        assertFalse(store.exists(prefix + "/a1/b2").join());

        zks.checkContainers();
        assertFalse(store.exists(prefix + "/a1").join());

        zks.checkContainers();
        assertFalse(store.exists(prefix).join());
    }

    @DataProvider(name = "conditionOfSwitchThread")
    public Object[][] conditionOfSwitchThread(){
        return new Object[][]{
            {false, false},
            {false, true},
            {true, false},
            {true, true}
        };
    }

    @Test(dataProvider = "conditionOfSwitchThread")
    public void testThreadSwitchOfZkMetadataStore(boolean hasSynchronizer, boolean enabledBatch) throws Exception {
        final String prefix = newKey();
        final String metadataStoreName = UUID.randomUUID().toString().replaceAll("-", "");
        MetadataStoreConfig.MetadataStoreConfigBuilder builder =
                MetadataStoreConfig.builder().metadataStoreName(metadataStoreName);
        builder.fsyncEnable(false);
        builder.batchingEnabled(enabledBatch);
        if (!hasSynchronizer) {
            builder.synchronizer(null);
        }
        MetadataStoreConfig config = builder.build();
        @Cleanup
        ZKMetadataStore store = (ZKMetadataStore) MetadataStoreFactory.create(zks.getConnectionString(), config);
        ZooKeeper zkClient = store.getZkClient();
        assertTrue(zkClient.getClientConfig().isSaslClientEnabled());
        final Runnable verify = () -> {
            String currentThreadName = Thread.currentThread().getName();
            String errorMessage = String.format("Expect to switch to thread %s, but currently it is thread %s",
                    metadataStoreName, currentThreadName);
            assertTrue(Thread.currentThread().getName().startsWith(metadataStoreName), errorMessage);
        };

        // put with node which has parent(but the parent node is not exists).
        store.put(prefix + "/a1/b1/c1", "value".getBytes(), Optional.of(-1L)).thenApply((ignore) -> {
            verify.run();
            return null;
        }).join();
        // put.
        store.put(prefix + "/b1", "value".getBytes(), Optional.of(-1L)).thenApply((ignore) -> {
            verify.run();
            return null;
        }).join();
        // get.
        store.get(prefix + "/b1").thenApply((ignore) -> {
            verify.run();
            return null;
        }).join();
        // get the node which is not exists.
        store.get(prefix + "/non").thenApply((ignore) -> {
            verify.run();
            return null;
        }).join();
        // delete.
        store.delete(prefix + "/b1", Optional.empty()).thenApply((ignore) -> {
            verify.run();
            return null;
        }).join();
        // delete the node which is not exists.
        store.delete(prefix + "/non", Optional.empty()).thenApply((ignore) -> {
            verify.run();
            return null;
        }).exceptionally(ex -> {
            verify.run();
            return null;
        }).join();
    }

    @Test
    public void testZkLoadConfigFromFile() throws Exception {
        final String metadataStoreName = UUID.randomUUID().toString().replaceAll("-", "");
        MetadataStoreConfig.MetadataStoreConfigBuilder builder =
                MetadataStoreConfig.builder().metadataStoreName(metadataStoreName);
        builder.fsyncEnable(false);
        builder.batchingEnabled(true);
        builder.configFilePath("src/test/resources/zk_client_disabled_sasl.conf");
        MetadataStoreConfig config = builder.build();
        @Cleanup
        ZKMetadataStore store = (ZKMetadataStore) MetadataStoreFactory.create(zks.getConnectionString(), config);

        PulsarZooKeeperClient zkClient = (PulsarZooKeeperClient) store.getZkClient();
        assertFalse(zkClient.getClientConfig().isSaslClientEnabled());

        zkClient.process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null));

        var zooKeeperRef = (AtomicReference<ZooKeeper>) WhiteboxImpl.getInternalState(zkClient, "zk");
        var zooKeeper = Awaitility.await().until(zooKeeperRef::get, Objects::nonNull);
        assertFalse(zooKeeper.getClientConfig().isSaslClientEnabled());
    }

    @Test
    public void testOxiaLoadConfigFromFile() throws Exception {
        final String metadataStoreName = UUID.randomUUID().toString().replaceAll("-", "");
        String oxia = "oxia://" + getOxiaServerConnectString();
        MetadataStoreConfig.MetadataStoreConfigBuilder builder =
                MetadataStoreConfig.builder().metadataStoreName(metadataStoreName);
        builder.fsyncEnable(false);
        builder.batchingEnabled(true);
        builder.sessionTimeoutMillis(30000);
        builder.configFilePath("src/test/resources/oxia_client.conf");
        MetadataStoreConfig config = builder.build();

        OxiaMetadataStore store = (OxiaMetadataStore) MetadataStoreFactory.create(oxia, config);
        var client = (AsyncOxiaClient) WhiteboxImpl.getInternalState(store, "client");
        var sessionManager = (SessionManager) WhiteboxImpl.getInternalState(client, "sessionManager");
        var sessionFactory = (SessionFactory) WhiteboxImpl.getInternalState(sessionManager, "factory");
        var clientConfig = (ClientConfig) WhiteboxImpl.getInternalState(sessionFactory, "config");
        var sessionTimeout = clientConfig.sessionTimeout();
        assertEquals(sessionTimeout, Duration.ofSeconds(60));
    }

    @Test(dataProvider = "impl")
    public void testPersistent(String provider, Supplier<String> urlSupplier) throws Exception {
        String metadataUrl = urlSupplier.get();
        MetadataStore store =
                MetadataStoreFactory.create(metadataUrl, MetadataStoreConfig.builder().fsyncEnable(false).build());
        byte[] data = "testPersistent".getBytes(StandardCharsets.UTF_8);

        String key = newKey() + "/a/b/c";
        store.put(key, data, Optional.of(-1L)).join();
        store.close();

        store = MetadataStoreFactory.create(metadataUrl, MetadataStoreConfig.builder().build());
        Optional<GetResult> result = store.get(key).get();
        assertTrue(result.isPresent());
        assertEquals(result.get().getValue(), data);
        store.close();
    }

    @Test(dataProvider = "impl")
    public void testConcurrentPutGetOneKey(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        byte[] data = new byte[]{0};
        String path = newKey();
        int maxValue = 100;
        store.put(path, data, Optional.of(-1L)).join();

        AtomicInteger successWrites = new AtomicInteger(0);
        Runnable task = new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                byte value;
                while (true) {
                    GetResult readResult = store.get(path).get().get();
                    value = (byte) (readResult.getValue()[0] + 1);
                    if (value <= maxValue) {
                        CompletableFuture<Void> putResult =
                                store.put(path, new byte[]{value}, Optional.of(readResult.getStat().getVersion()))
                                        .thenRun(successWrites::incrementAndGet);
                        try {
                            putResult.get();
                        } catch (Exception ignore) {
                        }
                        log.info("Put value {} success:{}. ", value, !putResult.isCompletedExceptionally());
                    } else {
                        break;
                    }
                }
            }
        };
        CompletableFuture<Void> t1 = CompletableFuture.completedFuture(null).thenRunAsync(task);
        CompletableFuture<Void> t2 = CompletableFuture.completedFuture(null).thenRunAsync(task);
        task.run();
        t1.join();
        t2.join();
        assertFalse(t1.isCompletedExceptionally());
        assertFalse(t2.isCompletedExceptionally());

        assertEquals(successWrites.get(), maxValue);
        assertEquals(store.get(path).get().get().getValue()[0], maxValue);
    }

    @Test(dataProvider = "impl")
    public void testConcurrentPut(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String k = newKey();
        CompletableFuture<Void> f1 =
                CompletableFuture.runAsync(() -> store.put(k, new byte[0], Optional.of(-1L)).join());
        CompletableFuture<Void> f2 =
                CompletableFuture.runAsync(() -> store.put(k, new byte[0], Optional.of(-1L)).join());
        Awaitility.await().until(() -> f1.isDone() && f2.isDone());
        assertTrue(f1.isCompletedExceptionally() && !f2.isCompletedExceptionally() ||
                ! f1.isCompletedExceptionally() && f2.isCompletedExceptionally());
    }

    @Test(dataProvider = "impl")
    public void testConcurrentDelete(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String k = newKey();
        store.put(k, new byte[0], Optional.of(-1L)).join();
        CompletableFuture<Void> f1 =
                CompletableFuture.runAsync(() -> store.delete(k, Optional.empty()).join());
        CompletableFuture<Void> f2 =
                CompletableFuture.runAsync(() -> store.delete(k, Optional.empty()).join());
        Awaitility.await().until(() -> f1.isDone() && f2.isDone());
        assertTrue(f1.isCompletedExceptionally() && !f2.isCompletedExceptionally() ||
                ! f1.isCompletedExceptionally() && f2.isCompletedExceptionally());
    }

    @Test(dataProvider = "impl")
    public void testGetChildren(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        store.put("/a/a-1", "value1".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        store.put("/a/a-2", "value1".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
        store.put("/b/c/b/1", "value1".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();

        List<String> subPaths = store.getChildren("/").get();
        Set<String> expectedSet = "ZooKeeper".equals(provider) ? Set.of("a", "b", "zookeeper") : Set.of("a", "b");
        for (String subPath : subPaths) {
            assertTrue(expectedSet.contains(subPath));
        }

        List<String> subPaths2 = store.getChildren("/a").get();
        Set<String> expectedSet2 = Set.of("a-1", "a-2");
        for (String subPath : subPaths2) {
            assertTrue(expectedSet2.contains(subPath));
        }

        List<String> subPaths3 = store.getChildren("/b").get();
        Set<String> expectedSet3 = Set.of("c");
        for (String subPath : subPaths3) {
            assertTrue(expectedSet3.contains(subPath));
        }
    }

    @Test(dataProvider = "impl")
    public void testClosedMetadataStore(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        store.close();
        try {
            store.get("/a").get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof MetadataStoreException.AlreadyClosedException);
        }
        try {
            store.put("/a", new byte[0], Optional.empty()).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof MetadataStoreException.AlreadyClosedException);
        }
        try {
            store.delete("/a", Optional.empty()).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof MetadataStoreException.AlreadyClosedException);
        }
        try {
            store.deleteRecursive("/a").get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof MetadataStoreException.AlreadyClosedException);
        }
        try {
            store.getChildren("/a").get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof MetadataStoreException.AlreadyClosedException);
        }
        try {
            store.exists("/a").get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof MetadataStoreException.AlreadyClosedException);
        }
    }

    @Test(dataProvider = "distributedImpl")
    public void testGetChildrenDistributed(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        @Cleanup
        MetadataStore store2 = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String parent = newKey();
        byte[] value = "value1".getBytes(StandardCharsets.UTF_8);
        store1.put(parent, value, Optional.empty()).get();
        store1.put(parent + "/a", value, Optional.empty()).get();
        assertEquals(store1.getChildren(parent).get(), List.of("a"));
        store1.delete(parent + "/a", Optional.empty()).get();
        assertEquals(store1.getChildren(parent).get(), Collections.emptyList());
        store1.delete(parent, Optional.empty()).get();
        assertEquals(store1.getChildren(parent).get(), Collections.emptyList());
        store2.put(parent + "/b", value, Optional.empty()).get();
        // There is a chance watcher event is not triggered before the store1.getChildren() call.
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertEquals(store1.getChildren(parent).get(), List.of("b")));
        store2.put(parent + "/c", value, Optional.empty()).get();
        Awaitility.await().atMost(3, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertEquals(store1.getChildren(parent).get(), List.of("b", "c")));
    }

    @Test(dataProvider = "distributedImpl")
    public void testExistsDistributed(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        @Cleanup
        MetadataStore store2 = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String parent = newKey();
        byte[] value = "value1".getBytes(StandardCharsets.UTF_8);
        assertFalse(store1.exists(parent).get());
        store1.put(parent, value, Optional.empty()).get();
        assertTrue(store1.exists(parent).get());
        assertFalse(store1.exists(parent + "/a").get());
        store2.put(parent + "/a", value, Optional.empty()).get();

        Awaitility.await()
                .untilAsserted(() -> assertTrue(store1.exists(parent + "/a").get()));

        // There is a chance watcher event is not triggered before the store1.exists() call.
        assertFalse(store1.exists(parent + "/b").get());
    }
}

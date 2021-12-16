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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
import org.assertj.core.util.Lists;
import org.testng.annotations.Test;

@Slf4j
public class MetadataStoreTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void emptyStoreTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        assertFalse(store.exists("/non-existing-key").join());
        assertFalse(store.exists("/non-existing-key/child").join());
        assertFalse(store.get("/non-existing-key").join().isPresent());
        assertFalse(store.get("/non-existing-key/child").join().isPresent());

        assertEquals(store.getChildren("/non-existing-key").join(), Collections.emptyList());
        assertEquals(store.getChildren("/non-existing-key/child").join(), Collections.emptyList());

        try {
            store.delete("/non-existing-key", Optional.empty()).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, NotFoundException.class);
        }

        try {
            store.delete("/non-existing-key", Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, NotFoundException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void insertionTestWithExpectedVersion(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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

        store.put(key1, "value-1".getBytes(), Optional.of(-1L)).join();

        assertTrue(store.exists(key1).join());
        Optional<GetResult> optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-1".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), 0);

        try {
            store.put(key1, "value-2".getBytes(), Optional.of(-1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        try {
            store.put(key1, "value-2".getBytes(), Optional.of(1L)).join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, BadVersionException.class);
        }

        assertTrue(store.exists(key1).join());
        optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-1".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), 0);

        store.put(key1, "value-2".getBytes(), Optional.of(0L)).join();

        assertTrue(store.exists(key1).join());
        optRes = store.get(key1).join();
        assertTrue(optRes.isPresent());
        assertEquals(optRes.get().getValue(), "value-2".getBytes());
        assertEquals(optRes.get().getStat().getVersion(), 1);
    }

    @Test(dataProvider = "impl")
    public void getChildrenTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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
        assertEquals(stat.getVersion(), 0);

        Notification n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.Created);
        assertEquals(n.getPath(), key1);

        // Trigger modified notification
        stat = store.put(key1, "value-2".getBytes(), Optional.empty()).join();
        n = notifications.poll(3, TimeUnit.SECONDS);
        assertNotNull(n);
        assertEquals(n.getType(), NotificationType.Modified);
        assertEquals(n.getPath(), key1);
        assertEquals(stat.getVersion(), 1);

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
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

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

    @Test(dataProvider = "impl")
    public void testPersistent(String provider, Supplier<String> urlSupplier) throws Exception {
        if (provider.equals("Memory")) {
            // Memory is not persistent.
            return;
        }
        String metadataUrl = urlSupplier.get();
        MetadataStore store = MetadataStoreFactory.create(metadataUrl, MetadataStoreConfig.builder().build());
        byte[] data = "testPersistent".getBytes(StandardCharsets.UTF_8);
        store.put("/a/b/c", data, Optional.of(-1L)).join();
        store.close();

        store = MetadataStoreFactory.create(metadataUrl, MetadataStoreConfig.builder().build());
        Optional<GetResult> result = store.get("/a/b/c").get();
        assertTrue(result.isPresent());
        assertEquals(result.get().getValue(), data);
        store.close();
    }

    @Test(dataProvider = "impl")
    public void testConcurrentPutGetOneKey(String provider, Supplier<String> urlSupplier) throws Exception {
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
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
}

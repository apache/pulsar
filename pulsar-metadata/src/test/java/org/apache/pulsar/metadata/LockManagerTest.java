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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class LockManagerTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void acquireLocks(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        @Cleanup
        LockManager<String> lockManager = coordinationService.getLockManager(String.class);

        String key = newKey();

        assertEquals(lockManager.listLocks(key).join(), Collections.emptyList());
        assertEquals(lockManager.readLock(key + "/1").join(), Optional.empty());

        ResourceLock<String> lock1 = lockManager.acquireLock(key + "/1", "lock-1").join();
        assertEquals(lockManager.listLocks(key).join(), Collections.singletonList("1"));
        assertEquals(lockManager.readLock(key + "/1").join(), Optional.of("lock-1"));
        assertEquals(lock1.getPath(), key + "/1");
        assertEquals(lock1.getValue(), "lock-1");

        CountDownLatch latchLock1 = new CountDownLatch(1);
        lock1.getLockExpiredFuture().thenRun(() -> {
            latchLock1.countDown();
        });
        assertEquals(latchLock1.getCount(), 1);

        assertEquals(lockManager.listLocks(key).join(), Collections.singletonList("1"));
        assertEquals(lockManager.readLock(key + "/1").join(), Optional.of("lock-1"));

        assertEquals(latchLock1.getCount(), 1);

        lock1.release().join();
        assertEquals(lockManager.listLocks(key).join(), Collections.emptyList());
        assertEquals(lockManager.readLock(key + "/1").join(), Optional.empty());

        // The future should have been triggered before the release is complete
        latchLock1.await(0, TimeUnit.SECONDS);

        // Double release shoud be a no-op
        lock1.release().join();

        ResourceLock<String> lock2 = lockManager.acquireLock(key + "/1", "lock-1").join();
        assertEquals(lockManager.listLocks(key).join(), Collections.singletonList("1"));
        assertEquals(lockManager.readLock(key + "/1").join(), Optional.of("lock-1"));
        assertEquals(lock2.getPath(), key + "/1");
        assertEquals(lock2.getValue(), "lock-1");
    }

    @Test(dataProvider = "impl")
    public void cleanupOnClose(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        LockManager<String> lockManager = coordinationService.getLockManager(String.class);

        String key = newKey();
        assertEquals(lockManager.listLocks(key).join(), Collections.emptyList());
        assertEquals(lockManager.readLock(key + "/1").join(), Optional.empty());

        lockManager.acquireLock(key + "/1", "lock-1").join();
        assertEquals(lockManager.listLocks(key).join(), Collections.singletonList("1"));
        assertEquals(lockManager.readLock(key + "/1").join(), Optional.of("lock-1"));

        lockManager.acquireLock(key + "/2", "lock-2").join();
        assertEquals(lockManager.listLocks(key + "").join(), new ArrayList<>(Arrays.asList("1", "2")));
        assertEquals(lockManager.readLock(key + "/2").join(), Optional.of("lock-2"));

        lockManager.close();

        lockManager = coordinationService.getLockManager(String.class);
        assertEquals(lockManager.listLocks(key).join(), Collections.emptyList());
        assertEquals(lockManager.readLock(key + "/1").join(), Optional.empty());
        assertEquals(lockManager.readLock(key + "/2").join(), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void updateValue(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        MetadataCache<String> cache = store.getMetadataCache(String.class);

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        @Cleanup
        LockManager<String> lockManager = coordinationService.getLockManager(String.class);

        String key = newKey();
        ResourceLock<String> lock = lockManager.acquireLock(key + "/1", "lock-1").join();
        assertEquals(lock.getValue(), "lock-1");
        assertEquals(cache.get(key + "/1").join().get(), "lock-1");

        lock.updateValue("value-2").join();
        assertEquals(lock.getValue(), "value-2");
        assertEquals(cache.get(key + "/1").join().get(), "value-2");
    }

    @Test(dataProvider = "impl")
    public void updateValueWhenVersionIsOutOfSync(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        MetadataCache<String> cache = store.getMetadataCache(String.class);

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        @Cleanup
        LockManager<String> lockManager = coordinationService.getLockManager(String.class);

        String key = newKey();
        ResourceLock<String> lock = lockManager.acquireLock(key + "/1", "lock-1").join();
        assertEquals(lock.getValue(), "lock-1");
        assertEquals(cache.get(key + "/1").join().get(), "lock-1");

        store.put(key + "/1",
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes("value-2"),
                Optional.empty(), EnumSet.of(CreateOption.Ephemeral)).join();

        lock.updateValue("value-2").join();
        assertEquals(lock.getValue(), "value-2");
        assertEquals(cache.get(key + "/1").join().get(), "value-2");
    }

    @Test(dataProvider = "impl")
    public void updateValueWhenKeyDisappears(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        MetadataCache<String> cache = store.getMetadataCache(String.class);

        @Cleanup
        CoordinationService coordinationService = new CoordinationServiceImpl(store);

        @Cleanup
        LockManager<String> lockManager = coordinationService.getLockManager(String.class);

        String key = newKey();
        ResourceLock<String> lock = lockManager.acquireLock(key + "/1", "lock-1").join();
        assertEquals(lock.getValue(), "lock-1");
        assertEquals(cache.get(key + "/1").join().get(), "lock-1");

        store.delete(key + "/1", Optional.empty()).join();

        lock.updateValue("value-2").join();
        assertEquals(lock.getValue(), "value-2");
        assertEquals(cache.get(key + "/1").join().get(), "value-2");
    }

    @Test(dataProvider = "impl")
    public void revalidateLockWithinSameSession(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store);
        @Cleanup
        LockManager<String> lm2 = cs2.getLockManager(String.class);

        String path1 = newKey();

        // Simulate existing lock with same content
        store.put(path1, "\"value-1\"".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral)).join();
        ResourceLock<String> rl2 = lm2.acquireLock(path1, "value-1").join();

        assertEquals(new String(store.get(path1).join().get().getValue()), "\"value-1\"");
        assertFalse(rl2.getLockExpiredFuture().isDone());

        String path2 = newKey();

        // Simulate existing lock with different content
        store.put(path2, "\"value-1\"".getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral)).join();
        rl2 = lm2.acquireLock(path2, "value-2").join();

        assertEquals(new String(store.get(path2).join().get().getValue()), "\"value-2\"");
        assertFalse(rl2.getLockExpiredFuture().isDone());
    }

    @Test(dataProvider = "impl")
    public void revalidateLockOnDifferentSession(String provider, Supplier<String> urlSupplier) throws Exception {
        if (provider.equals("Memory") || provider.equals("RocksDB")) {
            // Local memory provider doesn't really have the concept of multiple sessions
            return;
        }

        @Cleanup
        MetadataStoreExtended store1 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        @Cleanup
        MetadataStoreExtended store2 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(store1);
        @Cleanup
        LockManager<String> lm1 = cs1.getLockManager(String.class);

        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store2);
        @Cleanup
        LockManager<String> lm2 = cs2.getLockManager(String.class);

        String path1 = newKey();

        // Simulate existing lock with different content
        ResourceLock<String> rl1 = lm1.acquireLock(path1, "value-1").join();

        try {
            lm2.acquireLock(path1, "value-2").join();
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), LockBusyException.class);
        }

        // Lock-1 should not get invalidated
        assertFalse(rl1.getLockExpiredFuture().isDone());

        assertEquals(new String(store1.get(path1).join().get().getValue()), "\"value-1\"");

        // Simulate existing lock with same content. The 2nd acquirer will steal the lock
        String path2 = newKey();
        store1.put(path2, ObjectMapperFactory.getMapper().writer().writeValueAsBytes("value-1"), Optional.of(-1L),
                EnumSet.of(CreateOption.Ephemeral)).join();

        ResourceLock<String> rl2 = lm2.acquireLock(path2, "value-1").join();

        assertFalse(rl2.getLockExpiredFuture().isDone());

        Awaitility.await().untilAsserted(() -> {
            // On 'store1' we might see for a short amount of time an empty result still cached while the lock is
            // being reacquired.
            assertEquals(new String(store1.get(path2).join().get().getValue()), "\"value-1\"");
        });
    }

    @Test(dataProvider = "impl")
    public void testCleanUpStateWhenRevalidationGotLockBusy(String provider, Supplier<String> urlSupplier)
            throws Exception {

        if (provider.equals("Memory") || provider.equals("RocksDB")) {
            // Local memory provider doesn't really have the concept of multiple sessions
            return;
        }

        @Cleanup
        MetadataStoreExtended store1 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
        @Cleanup
        MetadataStoreExtended store2 = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(store1);
        @Cleanup
        LockManager<String> lm1 = cs1.getLockManager(String.class);

        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store2);
        @Cleanup
        LockManager<String> lm2 = cs2.getLockManager(String.class);

        String path1 = newKey();

        ResourceLock<String> lock1 = lm1.acquireLock(path1, "value-1").join();
        AtomicReference<ResourceLock<String>> lock2 = new AtomicReference<>();
        // lock 2 will steal the distributed lock first.
        Awaitility.await().until(()-> {
            // Ensure steal the lock success.
            try {
                lock2.set(lm2.acquireLock(path1, "value-1").join());
                return true;
            } catch (Exception ex) {
                return false;
            }
        });

        // Since we can steal the lock repeatedly, we don't know which one will get it.
        // But we can verify the final state.
        Awaitility.await().untilAsserted(() -> {
            if (lock1.getLockExpiredFuture().isDone()) {
                assertTrue(lm1.listLocks(path1).join().isEmpty());
                assertFalse(lock2.get().getLockExpiredFuture().isDone());
            } else if (lock2.get().getLockExpiredFuture().isDone()) {
                assertTrue(lm2.listLocks(path1).join().isEmpty());
                assertFalse(lock1.getLockExpiredFuture().isDone());
            } else {
                fail("unexpected behaviour");
            }
        });
    }
}

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
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.CustomLog;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.migration.MigrationPhase;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.DualMetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class DualMetadataCacheTest extends BaseMetadataStoreTest {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class TestObject {
        String name;
        int value;
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        super.setup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testCacheGetInNotStartedPhase() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        // Create object via source store
        String path = prefix + "/test-obj";
        TestObject obj = new TestObject("test", 42);
        cache.create(path, obj).join();

        // Read via cache
        Optional<TestObject> result = cache.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getName(), "test");
        assertEquals(result.get().getValue(), 42);
    }

    @Test
    public void testCacheGetWithStatsInNotStartedPhase() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        // Create object
        String path = prefix + "/test-obj";
        TestObject obj = new TestObject("test", 42);
        cache.create(path, obj).join();

        // Read with stats
        Optional<CacheGetResult<TestObject>> result = cache.getWithStats(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getValue().getName(), "test");
        assertEquals(result.get().getValue().getValue(), 42);
        assertNotNull(result.get().getStat());
    }

    @Test
    public void testCacheGetIfCached() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        String path = prefix + "/test-obj";

        // Initially not cached
        Optional<TestObject> cached = cache.getIfCached(path);
        assertEquals(cached, Optional.empty());

        // Create object
        TestObject obj = new TestObject("test", 42);
        cache.create(path, obj).join();

        // Now should be cached
        Optional<TestObject> cachedAfter = cache.getIfCached(path);
        assertTrue(cachedAfter.isPresent());
        assertEquals(cachedAfter.get().getName(), "test");
    }

    @Test
    public void testCacheGetChildren() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        // Create multiple objects
        cache.create(prefix + "/child1", new TestObject("obj1", 1)).join();
        cache.create(prefix + "/child2", new TestObject("obj2", 2)).join();
        cache.create(prefix + "/child3", new TestObject("obj3", 3)).join();

        // Get children
        var children = cache.getChildren(prefix).join();
        assertEquals(children.size(), 3);
        assertTrue(children.contains("child1"));
        assertTrue(children.contains("child2"));
        assertTrue(children.contains("child3"));
    }

    @Test
    public void testCacheExists() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        String path = prefix + "/test-obj";

        // Initially doesn't exist
        assertFalse(cache.exists(path).join());

        // Create object
        cache.create(path, new TestObject("test", 42)).join();

        // Now exists
        assertTrue(cache.exists(path).join());
    }

    @Test
    public void testCacheReadModifyUpdateOrCreate() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        String path = prefix + "/test-obj";

        // Create new object via readModifyUpdateOrCreate
        TestObject result1 = cache.readModifyUpdateOrCreate(path, optObj -> {
            assertFalse(optObj.isPresent());
            return new TestObject("created", 100);
        }).join();

        assertEquals(result1.getName(), "created");
        assertEquals(result1.getValue(), 100);

        // Modify existing object
        TestObject result2 = cache.readModifyUpdateOrCreate(path, optObj -> {
            assertTrue(optObj.isPresent());
            TestObject existing = optObj.get();
            return new TestObject(existing.getName() + "-modified", existing.getValue() + 1);
        }).join();

        assertEquals(result2.getName(), "created-modified");
        assertEquals(result2.getValue(), 101);
    }

    @Test
    public void testCacheReadModifyUpdate() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        String path = prefix + "/test-obj";

        // Create initial object
        cache.create(path, new TestObject("initial", 1)).join();

        // Modify it
        TestObject result = cache.readModifyUpdate(path, obj -> {
            return new TestObject(obj.getName() + "-updated", obj.getValue() * 2);
        }).join();

        assertEquals(result.getName(), "initial-updated");
        assertEquals(result.getValue(), 2);

        // Verify persisted
        Optional<TestObject> verified = cache.get(path).join();
        assertTrue(verified.isPresent());
        assertEquals(verified.get().getName(), "initial-updated");
        assertEquals(verified.get().getValue(), 2);
    }

    @Test
    public void testCachePutWithOptions() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        String path = prefix + "/test-obj";
        TestObject obj = new TestObject("test", 42);

        // Put without options (creates or updates)
        cache.put(path, obj, EnumSet.noneOf(CreateOption.class)).join();

        // Verify
        Optional<TestObject> result = cache.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getName(), "test");
    }

    @Test
    public void testCacheDelete() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        String path = prefix + "/test-obj";

        // Create object
        cache.create(path, new TestObject("test", 42)).join();
        assertTrue(cache.exists(path).join());

        // Delete
        cache.delete(path).join();
        assertFalse(cache.exists(path).join());
    }

    @Test
    public void testCacheWritesFailWhileSourceStoreIsReadOnly() throws Exception {
        String prefix = newKey();
        String path = prefix + "/test-obj";
        String newPath = prefix + "/new-obj";
        @Cleanup
        MetadataStore sourceStore =
                new ZKMetadataStore(zks.getConnectionString(), MetadataStoreConfig.builder().build(), true);

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);
        TestObject value = new TestObject("source", 1);
        putObject(sourceStore, path, value);

        String targetUrl = "memory:" + UUID.randomUUID();
        for (MigrationPhase phase : EnumSet.of(MigrationPhase.PREPARATION, MigrationPhase.COPYING)) {
            setMigrationState(sourceStore, phase, targetUrl);
            Awaitility.await().until(() -> dualStore.getMigrationPhase() == phase);

            TestObject newValue = new TestObject("new", 2);
            assertReadOnly("create", cache.create(newPath, newValue));
            assertReadOnly("put", cache.put(newPath, newValue, EnumSet.noneOf(CreateOption.class)));
            assertReadOnly("readModifyUpdateOrCreate", cache.readModifyUpdateOrCreate(newPath, __ -> newValue));
            assertReadOnly("readModifyUpdate", cache.readModifyUpdate(path, __ -> newValue));
            assertReadOnly("delete", cache.delete(path));

            assertFalse(sourceStore.exists(newPath).join());
            byte[] stored = sourceStore.get(path).join().orElseThrow().getValue();
            assertEquals(ObjectMapperFactory.getMapper().reader().readValue(stored, TestObject.class), value);
        }
    }

    private static void assertReadOnly(String operation, CompletableFuture<?> write) {
        Throwable error = write.handle((result, e) -> e).join();
        assertNotNull(error, operation + " succeeded while the source store is read-only");
        assertTrue(error instanceof IllegalStateException, operation + " failed with " + error);
        assertEquals(error.getMessage(), "Write operations not allowed during migrations");
    }

    @Test
    public void testPreparationWaitsForPendingCacheWrite() throws Exception {
        String path = newKey() + "/test-obj";
        // Hold the write in the source store until it's released
        CompletableFuture<Void> releaseWrite = new CompletableFuture<>();
        @Cleanup
        MetadataStore sourceStore = new ZKMetadataStore(zks.getConnectionString(),
                MetadataStoreConfig.builder().build(), true) {
            @Override
            protected CompletableFuture<Stat> storePut(String putPath, byte[] data, Optional<Long> expectedVersion,
                                                       Set<Option> opts) {
                if (!putPath.equals(path)) {
                    return super.storePut(putPath, data, expectedVersion, opts);
                }
                return releaseWrite.thenCompose(__ -> super.storePut(putPath, data, expectedVersion, opts));
            }
        };

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());
        CountDownLatch sessionLost = new CountDownLatch(1);
        dualStore.registerCancellableSessionListener(event -> {
            if (event == SessionEvent.SessionLost) {
                sessionLost.countDown();
            }
        });

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);
        CompletableFuture<Void> write = cache.create(path, new TestObject("test", 1));

        setMigrationState(sourceStore, MigrationPhase.PREPARATION, "memory:" + UUID.randomUUID());
        Awaitility.await().until(() -> dualStore.getMigrationPhase() == MigrationPhase.PREPARATION);
        // The preparation marks the session as lost after the pending source writes have completed
        assertFalse(sessionLost.await(1, TimeUnit.SECONDS), "The preparation didn't wait for the pending write");
        assertFalse(write.isDone());

        releaseWrite.complete(null);
        write.get(10, TimeUnit.SECONDS);
        assertTrue(sessionLost.await(10, TimeUnit.SECONDS));
        assertTrue(sourceStore.exists(path).join());
    }

    @Test
    public void testEphemeralCachePutIsRecreatedInTargetStore() throws Exception {
        String path = newKey() + "/test-obj";
        @Cleanup
        MetadataStore sourceStore =
                new ZKMetadataStore(zks.getConnectionString(), MetadataStoreConfig.builder().build(), true);

        String targetUrl = "memory:" + UUID.randomUUID();
        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(targetUrl,
                MetadataStoreConfig.builder().build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);
        cache.put(path, new TestObject("test", 1), EnumSet.of(CreateOption.Ephemeral)).join();

        setMigrationState(sourceStore, MigrationPhase.PREPARATION, targetUrl);
        // The store acknowledges the preparation after recreating its ephemeral nodes in the target store
        Awaitility.await().untilAsserted(() ->
                assertTrue(sourceStore.getChildren(MigrationState.PARTICIPANTS_PATH).join().isEmpty()));
        assertTrue(targetStore.exists(path).join(), "The ephemeral node wasn't recreated in the target store");
    }

    @DataProvider
    public Object[][] migrationOutcomes() {
        return new Object[][] {{MigrationPhase.FAILED}, {MigrationPhase.COMPLETED}};
    }

    @Test(dataProvider = "migrationOutcomes")
    public void testCacheWriteAfterMigrationEnds(MigrationPhase outcome) throws Exception {
        String path = newKey() + "/test-obj";
        @Cleanup
        MetadataStore sourceStore =
                new ZKMetadataStore(zks.getConnectionString(), MetadataStoreConfig.builder().build(), true);

        String targetUrl = "memory:" + UUID.randomUUID();
        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(targetUrl,
                MetadataStoreConfig.builder().build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());
        CountDownLatch sessionReestablished = new CountDownLatch(1);
        dualStore.registerCancellableSessionListener(event -> {
            if (event == SessionEvent.SessionReestablished) {
                sessionReestablished.countDown();
            }
        });
        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        setMigrationState(sourceStore, MigrationPhase.PREPARATION, targetUrl);
        Awaitility.await().untilAsserted(() ->
                assertTrue(sourceStore.getChildren(MigrationState.PARTICIPANTS_PATH).join().isEmpty()));
        // The session is reestablished when the migration ends, after the caches have been switched on completion
        setMigrationState(sourceStore, outcome, targetUrl);
        assertTrue(sessionReestablished.await(10, TimeUnit.SECONDS));

        cache.create(path, new TestObject("test", 1)).join();
        boolean completed = outcome == MigrationPhase.COMPLETED;
        assertTrue((completed ? targetStore : sourceStore).exists(path).join());
        assertFalse((completed ? sourceStore : targetStore).exists(path).join());
    }

    @Test
    public void testCacheInvalidate() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        String path = prefix + "/test-obj";

        // Create and cache object
        cache.create(path, new TestObject("test", 42)).join();
        cache.get(path).join(); // Ensure it's cached

        // Verify cached
        Optional<TestObject> cached = cache.getIfCached(path);
        assertTrue(cached.isPresent());

        // Invalidate
        cache.invalidate(path);

        // Should not be in cache anymore (but still in store)
        Optional<TestObject> cachedAfterInvalidate = cache.getIfCached(path);
        assertEquals(cachedAfterInvalidate, Optional.empty());

        // But should still exist in store
        Optional<TestObject> fromStore = cache.get(path).join();
        assertTrue(fromStore.isPresent());
    }

    @Test
    public void testCacheInvalidateAll() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        // Create multiple objects
        cache.create(prefix + "/obj1", new TestObject("test1", 1)).join();
        cache.create(prefix + "/obj2", new TestObject("test2", 2)).join();
        cache.create(prefix + "/obj3", new TestObject("test3", 3)).join();

        // Load into cache
        cache.get(prefix + "/obj1").join();
        cache.get(prefix + "/obj2").join();
        cache.get(prefix + "/obj3").join();

        // Invalidate all
        cache.invalidateAll();

        // None should be cached
        assertEquals(cache.getIfCached(prefix + "/obj1"), Optional.empty());
        assertEquals(cache.getIfCached(prefix + "/obj2"), Optional.empty());
        assertEquals(cache.getIfCached(prefix + "/obj3"), Optional.empty());
    }

    @Test
    public void testCacheRefresh() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        String path = prefix + "/test-obj";

        // Create object
        cache.create(path, new TestObject("test", 42)).join();
        cache.get(path).join(); // Ensure it's cached

        // Modify directly in store (bypassing cache)
        sourceStore.put(path,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(new TestObject("modified", 100)),
                Optional.empty()).join();

        // Refresh cache
        cache.refresh(path);

        // Wait a bit for refresh to complete
        Thread.sleep(200);

        // Should get updated value
        Optional<TestObject> result = cache.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getName(), "modified");
        assertEquals(result.get().getValue(), 100);
    }

    @Test
    public void testCacheSwitchToTargetStore() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String targetUrl = "memory:" + UUID.randomUUID();
        @Cleanup
        MetadataStore targetStore = MetadataStoreFactory.create(targetUrl,
                MetadataStoreConfig.builder().build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class);

        // Create object in source
        String path = prefix + "/test-obj";
        cache.create(path, new TestObject("source-obj", 1)).join();

        // Verify it exists in source
        assertTrue(cache.exists(path).join());

        // Trigger migration - first PREPARATION
        MigrationState preparationState = new MigrationState(MigrationPhase.PREPARATION, targetUrl);
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(preparationState),
                Optional.empty()).join();

        // Then COMPLETED phase
        MigrationState completedState = new MigrationState(MigrationPhase.COMPLETED, targetUrl);
        sourceStore.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(completedState),
                Optional.empty()).join();

        // Wait for dual store to switch and caches to update
        Thread.sleep(1000);

        // Create object in target via cache (should use target store now)
        String targetPath = prefix + "/target-obj";
        cache.create(targetPath, new TestObject("target-obj", 2)).join();

        // Verify it exists in target store directly
        Optional<byte[]> targetResult = targetStore.get(targetPath).join()
                .map(gr -> gr.getValue());
        assertTrue(targetResult.isPresent());
        TestObject targetObj = ObjectMapperFactory.getMapper().reader().readValue(targetResult.get(), TestObject.class);
        assertEquals(targetObj.getName(), "target-obj");
        assertEquals(targetObj.getValue(), 2);
    }

    @Test
    public void testMultipleCachesWithDifferentTypes() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        // Create caches for different types
        MetadataCache<TestObject> objCache = dualStore.getMetadataCache(TestObject.class);
        MetadataCache<String> strCache = dualStore.getMetadataCache(String.class);

        // Use both caches
        objCache.create(prefix + "/obj", new TestObject("test", 42)).join();
        strCache.create(prefix + "/str", "test-string").join();

        // Verify both work
        Optional<TestObject> objResult = objCache.get(prefix + "/obj").join();
        assertTrue(objResult.isPresent());
        assertEquals(objResult.get().getName(), "test");

        Optional<String> strResult = strCache.get(prefix + "/str").join();
        assertTrue(strResult.isPresent());
        assertEquals(strResult.get(), "test-string");
    }

    @Test
    public void testCacheWithCustomConfig() throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore sourceStore = MetadataStoreFactory.create(zks.getConnectionString(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        @Cleanup
        DualMetadataStore dualStore = new DualMetadataStore(sourceStore,
                MetadataStoreConfig.builder().build());

        // Create cache with custom config
        MetadataCacheConfig<TestObject> cacheConfig = MetadataCacheConfig.<TestObject>builder()
                .refreshAfterWriteMillis(1000)
                .build();

        MetadataCache<TestObject> cache = dualStore.getMetadataCache(TestObject.class, cacheConfig);

        // Use the cache
        String path = prefix + "/test-obj";
        cache.create(path, new TestObject("test", 42)).join();

        Optional<TestObject> result = cache.get(path).join();
        assertTrue(result.isPresent());
        assertEquals(result.get().getName(), "test");
    }

    private static void setMigrationState(MetadataStore store, MigrationPhase phase, String targetUrl)
            throws Exception {
        store.put(MigrationState.MIGRATION_FLAG_PATH,
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(new MigrationState(phase, targetUrl)),
                Optional.empty()).join();
    }

    private static void putObject(MetadataStore store, String path, TestObject value) throws Exception {
        store.put(path, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value), Optional.empty()).join();
    }
}

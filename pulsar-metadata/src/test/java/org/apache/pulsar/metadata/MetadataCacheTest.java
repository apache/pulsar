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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.BackoffBuilder;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.awaitility.Awaitility;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class MetadataCacheTest extends BaseMetadataStoreTest {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class MyClass {
        String a;
        int b;
    }

    @Test(dataProvider = "impl")
    public void emptyCacheTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        assertEquals(objCache.getIfCached("/non-existing-key"), Optional.empty());
        assertEquals(objCache.getIfCached("/non-existing-key/child"), Optional.empty());

        assertEquals(objCache.get("/non-existing-key").join(), Optional.empty());
        assertEquals(objCache.get("/non-existing-key/child").join(), Optional.empty());

        try {
            objCache.delete("/non-existing-key").join();
            fail("should have failed");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), NotFoundException.class);
        }

        try {
            objCache.delete("/non-existing-key/child").join();
            fail("should have failed");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), NotFoundException.class);
        }
    }

    @DataProvider(name = "zk")
    public Object[][] zkimplementations() {
        return new Object[][] {
            { "ZooKeeper", stringSupplier(() -> zks.getConnectionString()) },
        };
    }

    @Test(dataProvider = "zk")
    public void crossStoreAddDelete(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        MetadataStore store2 = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        MetadataStore store3 = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> objCache1 = store1.getMetadataCache(MyClass.class);
        MetadataCache<MyClass> objCache2 = store2.getMetadataCache(MyClass.class);
        MetadataCache<MyClass> objCache3 = store3.getMetadataCache(MyClass.class);

        List<MetadataCache<MyClass>> allCaches = new ArrayList<>();
        allCaches.add(objCache1);
        allCaches.add(objCache2);
        allCaches.add(objCache3);

        // Add on one cache and remove from another
        multiStoreAddDelete(allCaches, 0, 1, "add cache0 del cache1");
        // retry same order to rule out any stale state
        multiStoreAddDelete(allCaches, 0, 1, "add cache0 del cache1");
        // Reverse the operations
        multiStoreAddDelete(allCaches, 1, 0, "add cache1 del cache0");
        // Ensure that working on same cache continues to work.
        multiStoreAddDelete(allCaches, 1, 1, "add cache1 del cache1");
    }

    private void multiStoreAddDelete(List<MetadataCache<MyClass>> caches, int addOn, int delFrom, String testName)
            throws InterruptedException {
        MetadataCache<MyClass> addCache = caches.get(addOn);
        MetadataCache<MyClass> delCache = caches.get(delFrom);

        String key1 = "/test-key1";
        assertEquals(addCache.getIfCached(key1), Optional.empty());

        MyClass value1 = new MyClass(testName, 1);

        addCache.create(key1, value1).join();

        // all time for changes to propagate to other caches
        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            for (MetadataCache<MyClass> cache : caches) {
                if (cache == addCache) {
                    assertEquals(cache.getIfCached(key1), Optional.of(value1));
                }
                assertEquals(cache.get(key1).join(), Optional.of(value1));
                assertEquals(cache.getIfCached(key1), Optional.of(value1));
            }
        });

        delCache.delete(key1).join();

        // all time for changes to propagate to other caches
        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            // The entry should get removed from all caches
            for (MetadataCache<MyClass> cache : caches) {
                assertEquals(cache.getIfCached(key1), Optional.empty());
                assertEquals(cache.get(key1).join(), Optional.empty());
            }
        });
    }

    @Test(dataProvider = "zk")
    public void crossStoreUpdates(String provider, Supplier<String> urlSupplier) throws Exception {
        String testName = "cross store updates";
        @Cleanup
        MetadataStore store1 = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        @Cleanup
        MetadataStore store2 = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        MetadataCacheImpl<MyClass> objCache1 = (MetadataCacheImpl<MyClass>) store1.getMetadataCache(MyClass.class);

        MetadataCacheImpl<MyClass> objCache2 = (MetadataCacheImpl<MyClass>) store2.getMetadataCache(MyClass.class);
        AtomicReference<MyClass> storeObj = new AtomicReference<MyClass>();
        store2.registerListener(n -> {
            if (n.getType() == NotificationType.Modified) {
                CompletableFuture.runAsync(() -> {
                    try {
                        MyClass obj = objCache2.get(n.getPath()).get().get();
                        storeObj.set(obj);
                    } catch (Exception e) {
                        log.error("Got exception {}", e.getMessage());
                    }
                });
            }
        });

        String key1 = "/test-key1";
        assertEquals(objCache1.getIfCached(key1), Optional.empty());
        assertEquals(objCache2.getIfCached(key1), Optional.empty());

        MyClass value1 = new MyClass(testName, 1);
        objCache1.create(key1, value1).join();

        Awaitility.await().ignoreNoExceptions().untilAsserted(() -> {
            assertEquals(objCache1.getIfCached(key1), Optional.of(value1));
            assertEquals(objCache2.get(key1).join(), Optional.of(value1));
            assertEquals(objCache2.getIfCached(key1), Optional.of(value1));
        });

        MyClass value2 = new MyClass(testName, 2);
        objCache1.readModifyUpdate(key1, (oldData) -> value2).join();

        Awaitility.await().ignoreNoExceptions().untilAsserted(() ->assertEquals(storeObj.get(), value2));
    }

    @Test(dataProvider = "impl")
    public void insertionDeletionWitGenericType(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        MetadataCache<Map<String, String>> objCache = store.getMetadataCache(new TypeReference<Map<String, String>>() {
        });

        String key1 = newKey();

        assertEquals(objCache.getIfCached(key1), Optional.empty());

        Map<String, String> v = new TreeMap<>();
        v.put("a", "1");
        v.put("b", "2");
        objCache.create(key1, v).join();

        assertEqualsAndRetry(() -> objCache.getIfCached(key1), Optional.of(v), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.of(v));

        objCache.delete(key1).join();

        assertEquals(objCache.getIfCached(key1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void insertionDeletion(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        assertEquals(objCache.getIfCached(key1), Optional.empty());

        MyClass value1 = new MyClass("a", 1);
        objCache.create(key1, value1).join();

        MyClass value2 = new MyClass("a", 2);

        try {
            objCache.create(key1, value2).join();
            fail("should have failed to create");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), AlreadyExistsException.class);
        }

        assertEqualsAndRetry(() -> objCache.getIfCached(key1), Optional.of(value1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.of(value1));

        assertEquals(objCache.readModifyUpdateOrCreate(key1, __ -> value2).join(), value2);
        assertEquals(objCache.get(key1).join(), Optional.of(value2));
        assertEqualsAndRetry(() -> objCache.getIfCached(key1), Optional.of(value2), Optional.empty());

        objCache.delete(key1).join();

        assertEquals(objCache.getIfCached(key1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void insertionWithInvalidation(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        assertEquals(objCache.getIfCached(key1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.empty());

        MyClass value1 = new MyClass("a", 1);
        Stat putResult = store.put(key1, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value1),
                Optional.of(-1L)).join();
        assertTrue(putResult.isFirstVersion());

        Awaitility.await().untilAsserted(() -> {
            assertEquals(objCache.getIfCached(key1), Optional.of(value1));
            assertEquals(objCache.get(key1).join(), Optional.of(value1));
        });

        MyClass value2 = new MyClass("a", 2);
        store.put(key1, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value2),
                Optional.of(putResult.getVersion())).join();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(objCache.getIfCached(key1), Optional.of(value2));
            assertEquals(objCache.get(key1).join(), Optional.of(value2));
        });
    }

    @Test(dataProvider = "impl")
    public void insertionOutsideCache(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        assertEquals(objCache.getIfCached(key1), Optional.empty());
        assertEquals(objCache.get(key1).join(), Optional.empty());

        MyClass value1 = new MyClass("a", 1);
        store.put(key1, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value1), Optional.of(-1L)).join();

        assertEquals(objCache.get(key1).join(), Optional.of(value1));
        assertEqualsAndRetry(() -> objCache.getIfCached(key1), Optional.of(value1), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void updateOutsideCacheWithGenericType(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataCache<Map<String, String>> objCache = store.getMetadataCache(new TypeReference<Map<String, String>>() {
        });

        String key1 = newKey();
        objCache.get(key1);

        Map<String, String> v = new TreeMap<>();
        v.put("a", "1");
        v.put("b", "2");
        store.put(key1, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(v), Optional.of(-1L)).join();

        Awaitility.await().untilAsserted(() -> {
            assertEquals(objCache.getIfCached(key1), Optional.of(v));
            assertEquals(objCache.get(key1).join(), Optional.of(v));
        });
    }

    @Test(dataProvider = "impl")
    public void invalidJsonContent(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        store.put(key1, "-------".getBytes(), Optional.of(-1L)).join();

        try {
            objCache.get(key1).join();
            fail("should have failed to deserialize");
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), ContentDeserializationException.class);
        }
        assertEquals(objCache.getIfCached(key1), Optional.empty());
    }

    @Test(dataProvider = "impl")
    public void testReadCloned(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        MetadataCache<Policies> objCache = store.getMetadataCache(Policies.class);
        String path = "/testReadCloned-policies";
        // init cache
        Policies policies = new Policies();
        policies.max_unacked_messages_per_consumer = 100;
        policies.replication_clusters.add("1");
        objCache.create(path, policies).get();

        Policies tempPolicies = objCache.get(path).get().get();
        assertSame(tempPolicies, objCache.get(path).get().get());
        AtomicReference<Policies> reference = new AtomicReference<>(new Policies());
        AtomicReference<Policies> reference2 = new AtomicReference<>(new Policies());

        objCache.readModifyUpdate(path, (policies1) -> {
            assertNotSame(policies1, tempPolicies);
            reference.set(policies1);
            policies1.max_unacked_messages_per_consumer = 200;
            return policies1;
        }).get();
        objCache.readModifyUpdate(path, (policies1) -> {
            assertNotSame(policies1, tempPolicies);
            reference2.set(policies1);
            policies1.max_unacked_messages_per_consumer = 300;
            return policies1;
        }).get();
        //The original object should not be modified
        assertEquals(tempPolicies.max_unacked_messages_per_consumer.intValue(), 100);
        assertNotSame(reference.get(), reference2.get());
        assertNotEquals(reference.get().max_unacked_messages_per_consumer
                , reference2.get().max_unacked_messages_per_consumer);

    }

    @Test(dataProvider = "impl")
    public void testCloneInReadModifyUpdateOrCreate(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        MetadataCache<Policies> objCache = store.getMetadataCache(Policies.class);
        String path = "/testCloneInReadModifyUpdateOrCreate-policies";
        // init cache
        Policies policies = new Policies();
        policies.max_unacked_messages_per_consumer = 100;
        objCache.create(path, policies).get();

        Policies tempPolicies = objCache.get(path).get().get();
        assertSame(tempPolicies, objCache.get(path).get().get());
        AtomicReference<Policies> reference = new AtomicReference<>(new Policies());
        AtomicReference<Policies> reference2 = new AtomicReference<>(new Policies());

        objCache.readModifyUpdateOrCreate(path, (policies1) -> {
            Policies policiesRef = policies1.get();
            assertNotSame(policiesRef, tempPolicies);
            reference.set(policiesRef);
            policiesRef.max_unacked_messages_per_consumer = 200;
            return policiesRef;
        }).get();
        objCache.readModifyUpdateOrCreate(path, (policies1) -> {
            Policies policiesRef = policies1.get();
            assertNotSame(policiesRef, tempPolicies);
            reference2.set(policiesRef);
            policiesRef.max_unacked_messages_per_consumer = 300;
            return policiesRef;
        }).get();
        //The original object should not be modified
        assertEquals(tempPolicies.max_unacked_messages_per_consumer.intValue(), 100);
        assertNotSame(reference.get(), reference2.get());
        assertNotEquals(reference.get().max_unacked_messages_per_consumer
                , reference2.get().max_unacked_messages_per_consumer);

    }

    @Test(dataProvider = "impl")
    public void readModifyUpdate(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        MyClass value1 = new MyClass("a", 1);
        objCache.create(key1, value1).join();

        assertEquals(objCache.readModifyUpdate(key1, v -> new MyClass(v.a, v.b + 1)).join(),
                new MyClass("a", 2));

        Optional<MyClass> newValue1 = objCache.get(key1).join();
        assertTrue(newValue1.isPresent());
        assertEquals(newValue1.get().a, "a");
        assertEquals(newValue1.get().b, 2);

        // Should fail if the key does not exist
        try {
            objCache.readModifyUpdate(newKey(), v -> {
                return new MyClass(v.a, v.b + 1);
            }).join();
        } catch (CompletionException e) {
            assertEquals(e.getCause().getClass(), NotFoundException.class);
        }
    }

    /**
     * This test validates that metadata-cache can handle BadVersion failure if other cache/metadata-source updates the
     * data with different version.
     *
     * @throws Exception
     */
    @Test
    public void readModifyUpdateBadVersionRetry() throws Exception {
        String url = zks.getConnectionString();
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> cache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        MyClass value1 = new MyClass("a", 1);
        cache.create(key1, value1).join();
        assertEquals(cache.get(key1).join().get().b, 1);

        final var futures = new ArrayList<CompletableFuture<MyClass>>();
        final var sourceStores = new ArrayList<MetadataStore>();

        for (int i = 0; i < 20; i++) {
            final var sourceStore = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());
            sourceStores.add(sourceStore);
            final var objCache = sourceStore.getMetadataCache(MyClass.class);
            futures.add(objCache.readModifyUpdate(key1, v -> new MyClass(v.a, v.b + 1)));
        }
        FutureUtil.waitForAll(futures).join();
        for (var sourceStore : sourceStores) {
            sourceStore.close();
        }
    }

    @Test
    public void readModifyUpdateOrCreateRetryTimeout() throws Exception {
        String url = zks.getConnectionString();
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build());

        MetadataCache<MyClass> cache = store.getMetadataCache(MyClass.class, MetadataCacheConfig.builder()
                .retryBackoff(new BackoffBuilder()
                        .setInitialTime(5, TimeUnit.MILLISECONDS)
                        .setMax(1, TimeUnit.SECONDS)
                        .setMandatoryStop(3, TimeUnit.SECONDS)).build());

        Field metadataCacheField = cache.getClass().getDeclaredField("objCache");
        metadataCacheField.setAccessible(true);
        var objCache = metadataCacheField.get(cache);
        var spyObjCache = (AsyncLoadingCache<?, ?>) spy(objCache);
        doAnswer((Answer<CompletableFuture<MyClass>>) invocation -> CompletableFuture.failedFuture(
                new MetadataStoreException.BadVersionException(""))).when(spyObjCache).get(any());
        metadataCacheField.set(cache, spyObjCache);

        // Test three times to ensure that the retry works each time.
        for (int i = 0; i < 3; i++) {
            var start = System.currentTimeMillis();
            boolean timeouted = false;
            try {
                cache.readModifyUpdateOrCreate(newKey(), Optional::get).join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    var elapsed = System.currentTimeMillis() - start;
                    // Since we reduce the wait time by a random amount for each retry, the total elapsed time should be
                    // mandatoryStopTime - maxTime * 0.9, which is 2900ms.
                    assertTrue(elapsed >= 2900L,
                            "The elapsed time should be greater than the timeout. But now it's " + elapsed);
                    // The elapsed time should be less than the timeout. The 1.5 factor allows for some extra time.
                    assertTrue(elapsed < 3000L * 1.5,
                            "The retry should have been stopped after the timeout. But now it's " + elapsed);
                    timeouted = true;
                } else {
                    fail("Should have failed with TimeoutException, but failed with " + e.getCause());
                }
            }
            assertTrue(timeouted, "Should have failed with TimeoutException, but succeeded");
        }
    }

    @Test(dataProvider = "impl")
    public void getWithStats(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class);

        String key1 = newKey();

        MyClass value1 = new MyClass("a", 1);
        Stat stat1 = store.put(key1, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value1), Optional.of(-1L))
                .join();

        CacheGetResult<MyClass> res = objCache.getWithStats(key1).join().get();
        assertEquals(res.getValue(), value1);
        assertEquals(res.getStat().getVersion(), stat1.getVersion());
    }

    @Test(dataProvider = "impl")
    public void cacheWithCustomSerde(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        // Simple serde that convert numbers to ascii
        MetadataCache<Integer> objCache = store.getMetadataCache(new MetadataSerde<Integer>() {
            @Override
            public byte[] serialize(String path, Integer value) throws IOException {
                return value.toString().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public Integer deserialize(String path, byte[] content, Stat stat) throws IOException {
                return Integer.parseInt(new String(content, StandardCharsets.UTF_8));
            }
        });

        String key1 = newKey();

        objCache.create(key1, 1).join();

        assertEquals(objCache.get(key1).join().get(), (Integer) 1);
    }

    @Data
    @NoArgsConstructor
    static class CustomClass {
        @JsonIgnore
        private String path;

        public int a;
        public int b;
    }

    @Test(dataProvider = "impl")
    public void customSerde(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataCache<CustomClass> objCache = store.getMetadataCache(new MetadataSerde<CustomClass>() {
            @Override
            public byte[] serialize(String path, CustomClass value) throws IOException {
                return ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value);
            }

            @Override
            public CustomClass deserialize(String path, byte[] content, Stat stat) throws IOException {
                CustomClass cc = ObjectMapperFactory.getMapper().reader().readValue(content, CustomClass.class);
                cc.path = path;
                return cc;
            }
        });

        String key1 = newKey();

        CustomClass value1 = new CustomClass();
        value1.a = 1;
        value1.b = 2;
        Stat stat = store.put(key1, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value1), Optional.of(-1L))
                .join();

        CacheGetResult<CustomClass> res = objCache.getWithStats(key1).join().get();
        assertEquals(res.getStat().getVersion(), stat.getVersion());
        assertEquals(res.getValue().a, 1);
        assertEquals(res.getValue().b, 2);
        assertEquals(res.getValue().path, key1);
    }

    @Test(dataProvider = "distributedImpl")
    public void testPut(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup final var store1 = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder()
                .build());
        final var cache1 = store1.getMetadataCache(Integer.class);
        @Cleanup final var store2 = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder()
                .build());
        final var cache2 = store2.getMetadataCache(Integer.class);
        final var key = "/testPut";

        cache1.put(key, 1, EnumSet.of(CreateOption.Ephemeral)); // create
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cache1.get(key).get().orElse(-1), 1);
            assertEquals(cache2.get(key).get().orElse(-1), 1);
        });

        cache2.put(key, 2, EnumSet.of(CreateOption.Ephemeral)); // update
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cache1.get(key).get().orElse(-1), 2);
            assertEquals(cache2.get(key).get().orElse(-1), 2);
        });
    }

    @Test(dataProvider = "impl")
    public void testAsyncReloadConsumer(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        List<MyClass> refreshed = new ArrayList<>();
        MetadataCache<MyClass> objCache = store.getMetadataCache(MyClass.class,
                MetadataCacheConfig.<MyClass>builder().refreshAfterWriteMillis(100)
                        .asyncReloadConsumer((k, v) -> v.map(vv -> refreshed.add(vv.getValue()))).build());

        String key1 = newKey();

        MyClass value1 = new MyClass("a", 1);
        objCache.create(key1, value1);

        MyClass value2 = new MyClass("a", 2);
        store.put(key1, ObjectMapperFactory.getMapper().writer().writeValueAsBytes(value2), Optional.empty())
                .join();

        Awaitility.await().untilAsserted(() -> {
            refreshed.contains(value2);
        });
    }

    @Test
    public void testDefaultMetadataCacheConfig() {
        final var config = MetadataCacheConfig.builder().build();
        assertEquals(config.getRefreshAfterWriteMillis(), TimeUnit.MINUTES.toMillis(5));
        assertEquals(config.getExpireAfterWriteMillis(), TimeUnit.MINUTES.toMillis(10));
        final var backoff = config.getRetryBackoff().create();
        assertEquals(backoff.getInitial(), 5);
        assertEquals(backoff.getMax(), 3000);
        assertEquals(backoff.getMandatoryStop(), 30_000);
    }
}

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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.cache.impl.JSONMetadataSerdeSimpleType;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.apache.pulsar.metadata.tableview.impl.MetadataStoreTableViewImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MetadataStoreTableViewTest extends BaseMetadataStoreTest {

    LinkedBlockingDeque<Pair<String, Integer>> tails;
    LinkedBlockingDeque<Pair<String, Integer>> existings;

    @BeforeMethod
    void init(){
        tails = new LinkedBlockingDeque<>();
        existings = new LinkedBlockingDeque<>();
    }

    private void tailListener(String k, Integer v){
        tails.add(Pair.of(k, v));
    }

    private void existingListener(String k, Integer v){
        existings.add(Pair.of(k, v));
    }

    MetadataStoreTableViewImpl<Integer> createTestTableView(MetadataStore store, String prefix,
                                                            Supplier<String> urlSupplier)
            throws Exception {
        var tv = MetadataStoreTableViewImpl.<Integer>builder()
                .name("test")
                .clazz(Integer.class)
                .store(store)
                .pathPrefix(prefix)
                .conflictResolver((old, cur) -> {
                    if (old == null || cur == null) {
                        return true;
                    }
                    return old < cur;
                })
                .listenPathValidator((path) -> path.startsWith(prefix) && path.contains("my"))
                .tailItemListeners(List.of(this::tailListener))
                .existingItemListeners(List.of(this::existingListener))
                .timeoutInMillis(5_000)
                .build();
        tv.start();
        return tv;
    }

    private void assertGet(MetadataStoreTableViewImpl<Integer> tv, String path, Integer expected) {
        assertEquals(tv.get(path), expected);
        //Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(tv.get(path), expected));
    }


    @Test(dataProvider = "impl")
    public void emptyTableViewTest(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        assertFalse(tv.exists("non-existing-key"));
        assertFalse(tv.exists("non-existing-key/child"));
        assertNull(tv.get("non-existing-key"));
        assertNull(tv.get("non-existing-key/child"));

        try {
            tv.delete("non-existing-key").join();
            fail("Should have failed");
        } catch (CompletionException e) {
            assertException(e, NotFoundException.class);
        }

    }

    @Test(dataProvider = "impl")
    public void concurrentPutTest(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        int data = 1;
        String path = "my";
        int concurrent = 50;
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            futureList.add(tv.put(path, data).exceptionally(ex -> {
                if (!(ex.getCause() instanceof MetadataStoreTableViewImpl.ConflictException)) {
                    fail("fail to execute concurrent put", ex);
                }
                return null;
            }));
        }
        FutureUtil.waitForAll(futureList).join();

        assertGet(tv, path, data);
    }

    @Test(dataProvider = "impl")
    public void conflictResolverTest(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        String key1 = "my";

        tv.put(key1, 0).join();
        tv.put(key1, 0).exceptionally(ex -> {
            if (!(ex.getCause() instanceof MetadataStoreTableViewImpl.ConflictException)) {
                fail("fail to execute concurrent put", ex);
            }
            return null;
        }).join();
        assertGet(tv, key1, 0);
        tv.put(key1, 1).join();
        assertGet(tv, key1, 1);
        tv.put(key1, 0).exceptionally(ex -> {
            if (!(ex.getCause() instanceof MetadataStoreTableViewImpl.ConflictException)) {
                fail("fail to execute concurrent put", ex);
            }
            return null;
        }).join();
        assertGet(tv, key1, 1);
    }

    @Test(dataProvider = "impl")
    public void deleteTest(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        String key1 = "key";
        tv.put(key1, 0).join();
        tv.delete(key1).join();
        assertNull(tv.get(key1));
    }

    @Test(dataProvider = "impl")
    public void mapApiTest(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        assertTrue(tv.isEmpty());
        assertEquals(tv.size(), 0);

        String key1 = "my1";
        String key2 = "my2";

        int val1 = 1;
        int val2 = 2;

        tv.put(key1, val1).join();
        tv.put(key2, val2).join();
        assertGet(tv, key1, 1);
        assertGet(tv, key2, 2);

        assertFalse(tv.isEmpty());
        assertEquals(tv.size(), 2);

        List<String> actual = new ArrayList<>();
        tv.forEach((k, v) -> {
            actual.add(k + "," + v);
        });
        assertEquals(actual, List.of(key1 + "," + val1, key2 + "," + val2));

        var values = tv.values();
        assertEquals(values.size(), 2);
        assertTrue(values.containsAll(List.of(val1, val2)));

        var keys = tv.keySet();
        assertEquals(keys.size(), 2);
        assertTrue(keys.containsAll(List.of(key1, key2)));

        var entries = tv.entrySet();
        assertEquals(entries.size(), 2);
        assertTrue(entries.containsAll(Map.of(key1, val1, key2, val2).entrySet()));
    }

    @Test(dataProvider = "impl")
    public void notificationListeners(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        String keyPrefix = "tenant/ns";
        String key1 = keyPrefix + "/my-1";
        int val1 = 1;

        assertGet(tv, key1, null);

        // listen on put
        tv.put(key1, val1).join();
        var kv = tails.poll(3, TimeUnit.SECONDS);
        assertEquals(kv, Pair.of(key1, val1));
        assertEquals(tv.get(key1), val1);

        // listen on modified
        int val2 = 2;
        tv.put(key1, val2).join();
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertEquals(kv, Pair.of(key1, val2));
        assertEquals(tv.get(key1), val2);

        // no listen on the parent
        int val0 = 0;
        String childKey = key1 + "/my-child-1";
        tv.put(childKey, val0).join();
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertEquals(kv, Pair.of(childKey, val0));
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertNull(kv);
        assertEquals(tv.get(key1), val2);
        assertEquals(tv.get(childKey), val0);

        tv.put(childKey, val1).join();
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertEquals(kv, Pair.of(childKey, val1));
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertNull(kv);
        assertEquals(tv.get(key1), val2);
        assertEquals(tv.get(childKey), val1);

        tv.delete(childKey).join();
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertEquals(kv, Pair.of(childKey, null));
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertNull(kv);
        assertEquals(tv.get(key1), val2);
        assertNull(tv.get(childKey));

        // No listen on the filtered key
        String noListenKey = keyPrefix + "/to-be-filtered";
        tv.put(noListenKey, val0).join();
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertNull(kv);
        assertEquals(tv.get(key1), val2);
        assertNull(tv.get(noListenKey));

        // Trigger deleted notification
        tv.delete(key1).join();
        kv = tails.poll(3, TimeUnit.SECONDS);
        assertEquals(kv, Pair.of(key1, null));
        assertNull(tv.get(key1));
    }

    @Test(dataProvider = "impl")
    public void testConcurrentPutGetOneKey(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        String key = "my";
        int val = 0;
        int maxValue = 50;
        tv.put(key, val).join();

        AtomicInteger successWrites = new AtomicInteger(0);
        Runnable task = new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                for (int k = 0; k < 1000; k++) {
                    var kv = tails.poll(3, TimeUnit.SECONDS);
                    if (kv == null) {
                        break;
                    }
                    Integer val = kv.getRight() + 1;
                    if (val <= maxValue) {
                        CompletableFuture<Void> putResult =
                                tv.put(key, val).thenRun(successWrites::incrementAndGet);
                        try {
                            putResult.get();
                        } catch (Exception ignore) {
                        }
                        log.info("Put value {} success:{}. ", val, !putResult.isCompletedExceptionally());
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
        assertEquals(tv.get(key), maxValue);
    }

    @Test(dataProvider = "impl")
    public void testConcurrentPut(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        String k = "my";
        int v = 0;
        CompletableFuture<Void> f1 =
                CompletableFuture.runAsync(() -> tv.put(k, v).join());
        CompletableFuture<Void> f2 =
                CompletableFuture.runAsync(() -> tv.put(k, v).join());
        Awaitility.await().until(() -> f1.isDone() && f2.isDone());
        assertTrue(f1.isCompletedExceptionally() && !f2.isCompletedExceptionally() ||
                ! f1.isCompletedExceptionally() && f2.isCompletedExceptionally());
    }

    @Test(dataProvider = "impl")
    public void testConcurrentDelete(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);

        String k = "my";
        tv.put(k, 0).join();
        CompletableFuture<Void> f1 =
                CompletableFuture.runAsync(() -> tv.delete(k).join());
        CompletableFuture<Void> f2 =
                CompletableFuture.runAsync(() -> tv.delete(k).join());
        Awaitility.await().until(() -> f1.isDone() && f2.isDone());
        assertTrue(f1.isCompletedExceptionally() && !f2.isCompletedExceptionally() ||
                ! f1.isCompletedExceptionally() && f2.isCompletedExceptionally());
    }

    @Test(dataProvider = "impl")
    public void testClosedMetadataStore(String provider, Supplier<String> urlSupplier) throws Exception {
        String prefix = newKey();
        String k = "my";
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store, prefix, urlSupplier);
        store.close();
        try {
            tv.put(k, 0).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof MetadataStoreException.AlreadyClosedException);
        }
        try {
            tv.delete(k).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof MetadataStoreException.AlreadyClosedException);
        }

    }


    @Test(dataProvider = "distributedImpl")
    public void testGetIfCachedDistributed(String provider, Supplier<String> urlSupplier) throws Exception {

        String prefix = newKey();
        String k = "my";
        @Cleanup
        MetadataStore store1 = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv1 = createTestTableView(store1, prefix, urlSupplier);
        @Cleanup
        MetadataStore store2 = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv2 = createTestTableView(store2, prefix, urlSupplier);


        assertNull(tv1.get(k));
        assertNull(tv2.get(k));

        tv1.put(k, 0).join();
        assertGet(tv1, k, 0);
        Awaitility.await()
                .untilAsserted(() -> assertEquals(tv2.get(k), 0));

        tv2.put(k, 1).join();
        assertGet(tv2, k, 1);
        Awaitility.await()
                .untilAsserted(() -> assertEquals(tv1.get(k), 1));

        tv1.delete(k).join();
        assertGet(tv1, k, null);
        Awaitility.await()
                .untilAsserted(() -> assertNull(tv2.get(k)));
    }

    @Test(dataProvider = "distributedImpl")
    public void testInitialFill(String provider, Supplier<String> urlSupplier) throws Exception {

        String prefix = newKey();
        String k1 = "tenant-1/ns-1/my-1";
        String k2 = "tenant-1/ns-1/my-2";
        String k3 = "tenant-1/ns-2/my-3";
        String k4 = "tenant-2/ns-3/my-4";
        String k5 = "tenant-2/ns-3/your-1";
        @Cleanup
        MetadataStore store = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> btv = createTestTableView(store, prefix, urlSupplier);

        assertFalse(btv.exists(k1));

        var serde = new JSONMetadataSerdeSimpleType<>(
                TypeFactory.defaultInstance().constructSimpleType(Integer.class, null));
        store.put(prefix + "/" + k1, serde.serialize(prefix + "/" + k1, 0), Optional.empty()).join();
        store.put(prefix + "/" + k2, serde.serialize(prefix + "/" + k2, 1), Optional.empty()).join();
        store.put(prefix + "/" + k3, serde.serialize(prefix + "/" + k3, 2), Optional.empty()).join();
        store.put(prefix + "/" + k4, serde.serialize(prefix + "/" + k4, 3), Optional.empty()).join();
        store.put(prefix + "/" + k5, serde.serialize(prefix + "/" + k5, 4), Optional.empty()).join();

        var expected = new HashSet<>(Set.of(Pair.of(k1, 0), Pair.of(k2, 1), Pair.of(k3, 2), Pair.of(k4, 3)));
        var tailExpected = new HashSet<>(expected);

        for (int i = 0; i < 4; i++) {
            var kv = tails.poll(3, TimeUnit.SECONDS);
            assertTrue(tailExpected.remove(kv));
        }
        assertNull(tails.poll(3, TimeUnit.SECONDS));
        assertTrue(tailExpected.isEmpty());

        @Cleanup
        MetadataStore store2 = MetadataStoreFactoryImpl.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        MetadataStoreTableViewImpl<Integer> tv = createTestTableView(store2, prefix, urlSupplier);

        var existingExpected = new HashSet<>(Set.of(Pair.of(k1, 0), Pair.of(k2, 1), Pair.of(k3, 2), Pair.of(k4, 3)));
        var entrySetExpected = expected.stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight)).entrySet();


        for (int i = 0; i < 4; i++) {
            var kv = existings.poll(3, TimeUnit.SECONDS);
            assertTrue(existingExpected.remove(kv));
        }
        assertNull(existings.poll(3, TimeUnit.SECONDS));
        assertTrue(existingExpected.isEmpty());

        assertEquals(tv.get(k1), 0);
        assertEquals(tv.get(k2), 1);
        assertEquals(tv.get(k3), 2);
        assertEquals(tv.get(k4), 3);
        assertNull(tv.get(k5));

        assertEquals(tv.entrySet(), entrySetExpected);
    }
}

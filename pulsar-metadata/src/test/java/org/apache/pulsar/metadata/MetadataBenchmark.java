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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.Test;

@Slf4j
public class MetadataBenchmark extends MetadataStoreTest {

    @Test(dataProvider = "impl", enabled = false)
    public void testGet(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        final int N_KEYS = 128;
        final int N_GETS = 1_000_000;

        String key = newKey();
        generateKeys(store, key, N_KEYS);

        Semaphore s = new Semaphore(10_000);
        CountDownLatch latch = new CountDownLatch(N_GETS);

        long startTime = System.nanoTime();
        for (int i = 0; i < N_GETS; i++) {
            int k = i % (N_KEYS - 1);
            s.acquire();
            store.get(key + "/" + k)
                    .thenAccept(__ -> {
                        s.release();
                        latch.countDown();
                    }).exceptionally(ex -> {
                        log.warn("Failed to do get operation", ex);
                        return null;
                    });
        }

        latch.await();
        long endTime = System.nanoTime();
        double throughput = 1e9 * N_GETS / (endTime - startTime);

        log.info("[{}] Get Throughput: {} Kops/s", provider, throughput / 1_000);
    }

    @Test(dataProvider = "impl", enabled = false)
    public void testGetChildren(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        final int N_KEYS = 128;
        final int N_GETS = 1_000_000;

        String key = newKey();
        generateKeys(store, key, N_KEYS);

        Semaphore s = new Semaphore(10_000);
        CountDownLatch latch = new CountDownLatch(N_GETS);

        long startTime = System.nanoTime();
        for (int i = 0; i < N_GETS; i++) {
            s.acquire();
            store.getChildren(key)
                    .thenAccept(__ -> {
                        s.release();
                        latch.countDown();
                    }).exceptionally(ex -> {
                        log.warn("Failed to do get children operation", ex);
                        return null;
                    });
        }

        latch.await();
        long endTime = System.nanoTime();
        double throughput = 1e9 * N_GETS / (endTime - startTime);

        log.info("[{}] Get Children Throughput: {} Kops/s", provider, throughput / 1_000);
    }

    @Test(dataProvider = "impl", enabled = false)
    public void testPut(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder().build());

        final int N_KEYS = 10_000;
        final int N_PUTS = 100_000;

        String key = newKey();

        Semaphore s = new Semaphore(10_000);
        CountDownLatch latch = new CountDownLatch(N_PUTS);

        generateKeys(store, key, N_KEYS);

        long startTime = System.nanoTime();
        byte[] data = new byte[100];
        for (int i = 0; i < N_PUTS; i++) {
            int k = i % (N_KEYS - 1);
            s.acquire();
            store.put(key + "/" + k, data, Optional.empty())
                    .thenAccept(__ -> {
                        s.release();
                        latch.countDown();
                    }).exceptionally(ex -> {
                        log.warn("Failed to do put operation", ex);
                        return null;
                    });
        }

        latch.await();
        long endTime = System.nanoTime();
        double throughput = 1e9 * N_PUTS / (endTime - startTime);

        log.info("[{}] Put Throughput: {} Kops/s", provider, throughput / 1_000);
    }

    private void generateKeys(MetadataStore store, String prefix, int n) {
        byte[] payload = new byte[128];
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(store.put(prefix + "/" + i, payload, Optional.empty()));
        }

        FutureUtil.waitForAll(futures).join();
    }

}

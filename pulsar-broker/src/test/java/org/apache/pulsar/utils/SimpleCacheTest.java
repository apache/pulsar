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
package org.apache.pulsar.utils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class SimpleCacheTest {

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    @AfterClass
    public void shutdown() {
        executor.shutdown();
    }

    @Test
    public void testConcurrentUpdate() throws Exception {
        final var cache = new SimpleCache<Integer, Integer>(executor, 10000L, 10000L);
        final var pool = Executors.newFixedThreadPool(2);
        final var latch = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            final var value = i + 100;
            pool.execute(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
                cache.get(0, () -> value, __ -> {});
                latch.countDown();
            });
        }
        latch.await();
        final var value = cache.get(0, () -> -1, __ -> {});
        Assert.assertTrue(value == 100 || value == 101);
        pool.shutdown();
    }

    @Test
    public void testExpire() throws InterruptedException {
        final var cache = new SimpleCache<Integer, Integer>(executor, 500L, 5);
        final var expiredValues = Collections.synchronizedSet(new HashSet<Integer>());

        final var allKeys = IntStream.range(0, 5).boxed().collect(Collectors.toSet());
        allKeys.forEach(key -> cache.get(key, () -> key + 100, expiredValues::add));

        Thread.sleep(400L);
        final var recentAccessedKey = Set.of(1, 2);
        recentAccessedKey.forEach(key -> cache.get(key, () -> -1, expiredValues::add)); // access these keys

        Thread.sleep(300L);
        recentAccessedKey.forEach(key -> Assert.assertEquals(key + 100, cache.get(key, () -> -1, __ -> {})));
        allKeys.stream().filter(key -> !recentAccessedKey.contains(key))
                .forEach(key -> Assert.assertEquals(-1, cache.get(key, () -> -1, __ -> {})));
    }
}

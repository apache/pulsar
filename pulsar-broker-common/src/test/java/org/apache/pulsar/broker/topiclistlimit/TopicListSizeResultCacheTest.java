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
package org.apache.pulsar.broker.topiclistlimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicListSizeResultCacheTest {

    private TopicListSizeResultCache cache;
    private ExecutorService executorService;

    @BeforeMethod
    public void setup() {
        cache = new TopicListSizeResultCache();
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testGetTopicListSize_returnsSameInstanceForSameKey() {
        String namespace = "tenant/namespace";
        CommandGetTopicsOfNamespace.Mode mode = CommandGetTopicsOfNamespace.Mode.ALL;

        TopicListSizeResultCache.ResultHolder holder1 = cache.getTopicListSize(namespace, mode);
        TopicListSizeResultCache.ResultHolder holder2 = cache.getTopicListSize(namespace, mode);

        assertSame(holder1, holder2, "Should return the same ResultHolder instance for the same key");
    }

    @Test
    public void testGetTopicListSize_returnsDifferentInstancesForDifferentKeys() {
        String namespace1 = "tenant/namespace1";
        String namespace2 = "tenant/namespace2";
        CommandGetTopicsOfNamespace.Mode mode = CommandGetTopicsOfNamespace.Mode.ALL;

        TopicListSizeResultCache.ResultHolder holder1 = cache.getTopicListSize(namespace1, mode);
        TopicListSizeResultCache.ResultHolder holder2 = cache.getTopicListSize(namespace2, mode);

        assertNotSame(holder1, holder2, "Should return different ResultHolder instances for different namespaces");
    }

    @Test
    public void testGetTopicListSize_returnsDifferentInstancesForDifferentModes() {
        String namespace = "tenant/namespace";

        TopicListSizeResultCache.ResultHolder holder1 = cache.getTopicListSize(namespace,
                CommandGetTopicsOfNamespace.Mode.ALL);
        TopicListSizeResultCache.ResultHolder holder2 = cache.getTopicListSize(namespace,
                CommandGetTopicsOfNamespace.Mode.PERSISTENT);

        assertNotSame(holder1, holder2, "Should return different ResultHolder instances for different modes");
    }

    @Test
    public void testResultHolder_firstRequestReturnsInitialEstimate() throws Exception {
        TopicListSizeResultCache.ResultHolder holder = new TopicListSizeResultCache.ResultHolder();

        CompletableFuture<Long> sizeFuture = holder.getSizeAsync();

        assertTrue(sizeFuture.isDone(), "First request should complete immediately");
        assertEquals(sizeFuture.get().longValue(), 10 * 1024L,
                "First request should return initial estimate of 10KB");
    }

    @Test
    public void testResultHolder_concurrentRequestsWaitForFirstToComplete() throws Exception {
        TopicListSizeResultCache.ResultHolder holder = new TopicListSizeResultCache.ResultHolder();
        int numConcurrentRequests = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numConcurrentRequests);

        List<CompletableFuture<Long>> futures = new ArrayList<>();

        // Start concurrent requests
        for (int i = 0; i < numConcurrentRequests; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    CompletableFuture<Long> future = holder.getSizeAsync();
                    futures.add(future);
                    completeLatch.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Trigger all requests
        startLatch.countDown();
        completeLatch.await(5, TimeUnit.SECONDS);

        // First request completes immediately with initial estimate
        assertTrue(futures.get(0).isDone());
        assertEquals(futures.get(0).get().longValue(), 10 * 1024L);

        // Other requests should be waiting
        for (int i = 1; i < numConcurrentRequests; i++) {
            assertFalse(futures.get(i).isDone(), "Concurrent request " + i + " should be waiting");
        }

        // Update size to complete waiting requests
        long actualSize = 20 * 1024L;
        holder.updateSize(actualSize);

        // All waiting requests should now complete with the actual size
        for (int i = 1; i < numConcurrentRequests; i++) {
            assertTrue(futures.get(i).isDone(), "Request " + i + " should complete after updateSize");
            assertEquals(futures.get(i).get().longValue(), actualSize);
        }
    }

    @Test
    public void testResultHolder_updateSize_firstUpdate() {
        TopicListSizeResultCache.ResultHolder holder = new TopicListSizeResultCache.ResultHolder();

        // Get initial size
        holder.getSizeAsync();

        // Update with actual size
        long actualSize = 20 * 1024L;
        holder.updateSize(actualSize);

        // Next request should return the actual size
        CompletableFuture<Long> nextFuture = holder.getSizeAsync();
        assertEquals(nextFuture.join().longValue(), actualSize);
    }

    @Test
    public void testResultHolder_updateSize_completesWaitingFuture() throws Exception {
        TopicListSizeResultCache.ResultHolder holder = new TopicListSizeResultCache.ResultHolder();

        // First request gets initial estimate
        CompletableFuture<Long> first = holder.getSizeAsync();
        assertTrue(first.isDone());

        // Second request should wait
        CompletableFuture<Long> second = holder.getSizeAsync();
        assertFalse(second.isDone(), "Second request should be waiting");

        // Update size should complete the waiting future
        long actualSize = 15000L;
        holder.updateSize(actualSize);

        assertTrue(second.isDone(), "Update should complete waiting future");
        assertEquals(second.get().longValue(), actualSize);
    }

    @Test
    public void testResultHolder_resetIfInitializing_resetsUncompletedFuture() throws Exception {
        TopicListSizeResultCache.ResultHolder holder = new TopicListSizeResultCache.ResultHolder();

        // First request gets initial estimate
        holder.getSizeAsync();

        // Second request should wait
        CompletableFuture<Long> waitingFuture = holder.getSizeAsync();
        assertFalse(waitingFuture.isDone(), "Second request should be waiting");

        // Reset should complete the waiting future with initial estimate
        holder.resetIfInitializing();

        assertTrue(waitingFuture.isDone(), "Reset should complete waiting future");
        assertEquals(waitingFuture.get().longValue(), 10 * 1024L,
                "Reset should complete with initial estimate");

        // Next request should start fresh
        CompletableFuture<Long> afterReset = holder.getSizeAsync();
        assertTrue(afterReset.isDone(), "Request after reset should complete immediately");
        assertEquals(afterReset.get().longValue(), 10 * 1024L);
    }

    @Test
    public void testResultHolder_resetIfInitializing_noEffectOnCompletedFuture() throws Exception {
        TopicListSizeResultCache.ResultHolder holder = new TopicListSizeResultCache.ResultHolder();

        // Get initial size and update
        holder.getSizeAsync();
        holder.updateSize(20000L);

        CompletableFuture<Long> future = holder.getSizeAsync();
        assertTrue(future.isDone());
        long valueBefore = future.get();

        // Reset should have no effect on completed future
        holder.resetIfInitializing();

        CompletableFuture<Long> futureAfter = holder.getSizeAsync();
        assertEquals(futureAfter.join().longValue(), valueBefore,
                "Reset should not affect already completed future");
    }

    @Test
    public void testResultHolder_multipleSequentialRequests() throws Exception {
        TopicListSizeResultCache.ResultHolder holder = new TopicListSizeResultCache.ResultHolder();

        // First request
        CompletableFuture<Long> future1 = holder.getSizeAsync();
        assertEquals(future1.get().longValue(), 10 * 1024L);

        // Update
        holder.updateSize(15000L);

        // Second request
        CompletableFuture<Long> future2 = holder.getSizeAsync();
        assertEquals(future2.get().longValue(), 15000L);

        // Update again
        holder.updateSize(25000L);

        // Third request
        CompletableFuture<Long> future3 = holder.getSizeAsync();
        assertEquals(future3.get().longValue(), 25000L, "Should be the last value");
    }

    @Test
    public void testCacheKey_equalityForSameValues() {
        TopicListSizeResultCache.CacheKey key1 = new TopicListSizeResultCache.CacheKey("tenant/ns",
                CommandGetTopicsOfNamespace.Mode.ALL);
        TopicListSizeResultCache.CacheKey key2 = new TopicListSizeResultCache.CacheKey("tenant/ns",
                CommandGetTopicsOfNamespace.Mode.ALL);

        assertEquals(key1, key2, "Keys with same values should be equal");
        assertEquals(key1.hashCode(), key2.hashCode(), "Equal keys should have same hash code");
    }

    @Test
    public void testCacheKey_inequalityForDifferentNamespaces() {
        TopicListSizeResultCache.CacheKey key1 = new TopicListSizeResultCache.CacheKey("tenant/ns1",
                CommandGetTopicsOfNamespace.Mode.ALL);
        TopicListSizeResultCache.CacheKey key2 = new TopicListSizeResultCache.CacheKey("tenant/ns2",
                CommandGetTopicsOfNamespace.Mode.ALL);

        assertNotEquals(key1, key2, "Keys with different namespaces should not be equal");
    }

    @Test
    public void testCacheKey_inequalityForDifferentModes() {
        TopicListSizeResultCache.CacheKey key1 = new TopicListSizeResultCache.CacheKey("tenant/ns",
                CommandGetTopicsOfNamespace.Mode.ALL);
        TopicListSizeResultCache.CacheKey key2 = new TopicListSizeResultCache.CacheKey("tenant/ns",
                CommandGetTopicsOfNamespace.Mode.PERSISTENT);

        assertNotEquals(key1, key2, "Keys with different modes should not be equal");
    }

    @Test
    public void testHighConcurrency() throws Exception {
        TopicListSizeResultCache.ResultHolder holder = new TopicListSizeResultCache.ResultHolder();
        int numThreads = 20;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);

        List<CompletableFuture<Long>> futures = new ArrayList<>();

        // Launch many concurrent requests
        for (int i = 0; i < numThreads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    CompletableFuture<Long> future = holder.getSizeAsync();
                    synchronized (futures) {
                        futures.add(future);
                    }
                    completeLatch.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(5, TimeUnit.SECONDS), "All requests should complete");

        // Update to complete all waiting requests
        holder.updateSize(30000L);

        // Verify all futures complete successfully
        for (CompletableFuture<Long> future : futures) {
            assertNotNull(future.get(5, TimeUnit.SECONDS), "All futures should complete");
        }
    }
}
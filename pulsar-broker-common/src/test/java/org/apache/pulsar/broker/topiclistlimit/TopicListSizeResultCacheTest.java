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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.awaitility.Awaitility;
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
    public void testGetTopicListSize_keysByProperties() {
        String namespace = "tenant/namespace";
        CommandGetTopicsOfNamespace.Mode mode = CommandGetTopicsOfNamespace.Mode.PERSISTENT;

        TopicListSizeResultCache.ResultHolder noProperties = cache.getTopicListSize(namespace, mode);
        assertSame(cache.getTopicListSize(namespace, mode, null), noProperties);
        assertSame(cache.getTopicListSize(namespace, mode, Map.of()), noProperties);

        Map<String, String> properties = new HashMap<>();
        properties.put("k1", "v1");
        properties.put("k2", "v2");
        TopicListSizeResultCache.ResultHolder withProperties = cache.getTopicListSize(namespace, mode, properties);
        assertNotSame(withProperties, noProperties,
                "A customized topic listing with properties should have a separate size estimate");
        assertSame(cache.getTopicListSize(namespace, mode, Map.of("k2", "v2", "k1", "v1")), withProperties);
        // the key must not be affected by later modifications of the passed map
        properties.put("k3", "v3");
        assertSame(cache.getTopicListSize(namespace, mode, Map.of("k2", "v2", "k1", "v1")), withProperties);
        assertNotSame(cache.getTopicListSize(namespace, mode, Map.of("k1", "other")), withProperties);
    }

    @Test
    public void testGetTopicListSize_keysByListing() {
        String namespace = "tenant/namespace";
        CommandGetTopicsOfNamespace.Mode mode = CommandGetTopicsOfNamespace.Mode.NON_PERSISTENT;

        TopicListSizeResultCache.ResultHolder namespaceTopics = cache.getTopicListSize(namespace, mode);
        assertSame(cache.getTopicListSize(TopicListing.namespaceTopics(namespace, mode), null), namespaceTopics);

        List<TopicListing> otherListings = List.of(
                TopicListing.ownedBundleTopics(namespace, mode, "0x00000000_0x80000000"),
                TopicListing.ownedBundleTopics(namespace, mode, "0x80000000_0xffffffff"),
                TopicListing.partitionedTopics(namespace, mode),
                TopicListing.scalableTopics(namespace));
        List<TopicListSizeResultCache.ResultHolder> holders = new ArrayList<>();
        holders.add(namespaceTopics);
        for (TopicListing listing : otherListings) {
            TopicListSizeResultCache.ResultHolder holder = cache.getTopicListSize(listing, null);
            assertSame(cache.getTopicListSize(listing, Map.of()), holder);
            for (TopicListSizeResultCache.ResultHolder other : holders) {
                assertNotSame(holder, other, "Each topic listing should have a separate size estimate: " + listing);
            }
            holders.add(holder);
        }
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

        // Use a thread-safe list since multiple threads call add() concurrently.
        List<CompletableFuture<Long>> futures = new CopyOnWriteArrayList<>();

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

        // Exactly one request (the first to call getSizeAsync) completes immediately
        // with the initial estimate. The rest wait for updateSize(). We can't assume
        // which index in the list corresponds to the "first" caller since threads
        // race to add their futures to the list.
        long doneCount = futures.stream().filter(CompletableFuture::isDone).count();
        assertEquals(doneCount, 1, "Exactly one request should be done (the first caller)");

        long waitingCount = futures.stream().filter(f -> !f.isDone()).count();
        assertEquals(waitingCount, numConcurrentRequests - 1, "All other requests should be waiting");

        CompletableFuture<Long> firstFuture = futures.stream()
                .filter(CompletableFuture::isDone).findFirst().orElseThrow();
        assertEquals(firstFuture.get().longValue(), 10 * 1024L);

        // Update size to complete waiting requests
        long actualSize = 20 * 1024L;
        holder.updateSize(actualSize);

        // All waiting requests should now complete with the actual size
        for (CompletableFuture<Long> f : futures) {
            assertTrue(f.isDone(), "All requests should be done after updateSize");
        }
        for (CompletableFuture<Long> f : futures) {
            long value = f.get().longValue();
            assertTrue(value == 10 * 1024L || value == actualSize,
                    "Each future should have either the initial estimate or the actual size");
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

    @Test
    public void testOversizedEstimateFailsFastUntilIntervalHasPassed() {
        TopicListSizeResultCache limitedCache =
                new TopicListSizeResultCache(100_000, Duration.ofHours(1), Duration.ofHours(1), () -> true);
        TopicListSizeResultCache.ResultHolder holder =
                limitedCache.getTopicListSize("tenant/namespace", CommandGetTopicsOfNamespace.Mode.PERSISTENT);
        holder.getSizeAsync();
        // a topic list larger than the limit
        holder.updateSize(500_000);
        // the requests get the estimate, which is larger than the limit, so that they fail right away instead of
        // listing the topics again right after the oversized list was measured
        assertEquals(holder.getSizeAsync().join().longValue(), 500_000L);
        assertEquals(holder.getSizeAsync().join().longValue(), 500_000L);
        // the estimate follows the list when it becomes smaller than the limit
        holder.updateSize(50_000);
        assertEquals(holder.getSizeAsync().join().longValue(), 50_000L);
    }

    @Test
    public void testOversizedEstimateIsRemeasuredOnlyWhenLimiterIsIdle() {
        AtomicBoolean limiterIdle = new AtomicBoolean(false);
        TopicListSizeResultCache limitedCache =
                new TopicListSizeResultCache(100_000, Duration.ZERO, Duration.ofHours(1), limiterIdle::get);
        TopicListSizeResultCache.ResultHolder holder =
                limitedCache.getTopicListSize("tenant/namespace", CommandGetTopicsOfNamespace.Mode.PERSISTENT);
        holder.getSizeAsync();
        holder.updateSize(500_000);
        // other topic lists are in flight: requesting all permits would make the later requests wait behind it
        assertEquals(holder.getSizeAsync().join().longValue(), 500_000L);
        // no other topic list is in flight: the request gets the limit so that it can measure the list again
        limiterIdle.set(true);
        assertEquals(holder.getSizeAsync().join().longValue(), 100_000L);
    }

    @Test
    public void testNewPropertiesStartFromEstimateWithoutProperties() {
        String namespace = "tenant/namespace";
        CommandGetTopicsOfNamespace.Mode mode = CommandGetTopicsOfNamespace.Mode.PERSISTENT;
        // without a known estimate, a listing with properties starts from the initial estimate
        TopicListSizeResultCache.ResultHolder first = cache.getTopicListSize(namespace, mode, Map.of("k", "v0"));
        assertEquals(first.getSizeAsync().join().longValue(), 10 * 1024L);

        TopicListSizeResultCache.ResultHolder withoutProperties = cache.getTopicListSize(namespace, mode);
        withoutProperties.getSizeAsync();
        withoutProperties.updateSize(500_000);
        // a listing with new properties can't start below the known size of the listing without properties
        TopicListSizeResultCache.ResultHolder withNewProperties =
                cache.getTopicListSize(namespace, mode, Map.of("k", "v1"));
        assertEquals(withNewProperties.getSizeAsync().join().longValue(), 500_000L);
        // and then follows its own topic list
        withNewProperties.updateSize(1000);
        assertEquals(withNewProperties.getSizeAsync().join().longValue(), 1000L);
        assertEquals(withoutProperties.getSizeAsync().join().longValue(), 500_000L);
    }

    @Test
    public void testNewPropertiesWithOversizedEstimateFailFastWhileLimiterIsBusy() {
        AtomicBoolean limiterIdle = new AtomicBoolean(false);
        TopicListSizeResultCache limitedCache =
                new TopicListSizeResultCache(100_000, Duration.ofHours(1), Duration.ofHours(1), limiterIdle::get);
        String namespace = "tenant/namespace";
        CommandGetTopicsOfNamespace.Mode mode = CommandGetTopicsOfNamespace.Mode.PERSISTENT;
        TopicListSizeResultCache.ResultHolder withoutProperties = limitedCache.getTopicListSize(namespace, mode);
        withoutProperties.getSizeAsync();
        withoutProperties.updateSize(500_000);

        TopicListSizeResultCache.ResultHolder withNewProperties =
                limitedCache.getTopicListSize(namespace, mode, Map.of("k", "v1"));
        assertEquals(withNewProperties.getSizeAsync().join().longValue(), 500_000L);
        // the request is rejected, which resets the estimate that it was initializing
        withNewProperties.resetIfInitializing();
        // a listing with new properties hasn't been measured, so it may be measured when the limiter is idle
        limiterIdle.set(true);
        assertEquals(withNewProperties.getSizeAsync().join().longValue(), 100_000L);
    }

    @Test
    public void testOnlyOneOversizedListingIsRemeasuredAtATime() {
        TopicListSizeResultCache limitedCache =
                new TopicListSizeResultCache(100_000, Duration.ZERO, Duration.ofHours(1), () -> true);
        TopicListSizeResultCache.ResultHolder first =
                limitedCache.getTopicListSize("tenant/ns1", CommandGetTopicsOfNamespace.Mode.PERSISTENT);
        TopicListSizeResultCache.ResultHolder second =
                limitedCache.getTopicListSize("tenant/ns2", CommandGetTopicsOfNamespace.Mode.PERSISTENT);
        for (TopicListSizeResultCache.ResultHolder holder : List.of(first, second)) {
            holder.getSizeAsync();
            holder.updateSize(500_000);
        }
        // the first listing is measured again, the second one fails fast while that's in progress
        assertEquals(first.getSizeAsync().join().longValue(), 100_000L);
        assertEquals(second.getSizeAsync().join().longValue(), 500_000L);
        // the first listing measured its size, so the second listing may be measured again
        first.updateSize(500_000);
        assertEquals(second.getSizeAsync().join().longValue(), 100_000L);
    }

    @Test
    public void testOversizedEstimateIsRemeasuredWhenLimiterIsNeverIdle() {
        TopicListSizeResultCache limitedCache =
                new TopicListSizeResultCache(100_000, Duration.ZERO, Duration.ZERO, () -> false);
        TopicListSizeResultCache.ResultHolder holder =
                limitedCache.getTopicListSize("tenant/namespace", CommandGetTopicsOfNamespace.Mode.PERSISTENT);
        holder.getSizeAsync();
        holder.updateSize(500_000);
        // after the longer interval, the listing is measured again although other topic lists are in flight
        assertEquals(holder.getSizeAsync().join().longValue(), 100_000L);
    }

    @Test
    public void testForcedRemeasuresAreLimitedForTheWholeCache() throws Exception {
        Duration forcedInterval = Duration.ofMillis(500);
        TopicListSizeResultCache limitedCache =
                new TopicListSizeResultCache(100_000, Duration.ZERO, forcedInterval, () -> false);
        // listings with different properties have separate estimates
        TopicListSizeResultCache.ResultHolder first = limitedCache.getTopicListSize("tenant/namespace",
                CommandGetTopicsOfNamespace.Mode.PERSISTENT, Map.of("k", "v1"));
        TopicListSizeResultCache.ResultHolder second = limitedCache.getTopicListSize("tenant/namespace",
                CommandGetTopicsOfNamespace.Mode.PERSISTENT, Map.of("k", "v2"));
        for (TopicListSizeResultCache.ResultHolder holder : List.of(first, second)) {
            holder.getSizeAsync();
            holder.updateSize(500_000);
        }
        Thread.sleep(forcedInterval.toMillis() + 100);
        // the limiter is busy: the first listing is measured again after the longer interval
        assertEquals(first.getSizeAsync().join().longValue(), 100_000L);
        first.updateSize(500_000);
        // the second listing's interval has also passed, but forced re-measures are limited for the whole cache
        assertEquals(second.getSizeAsync().join().longValue(), 500_000L);
        Awaitility.await().untilAsserted(
                () -> assertEquals(second.getSizeAsync().join().longValue(), 100_000L));
    }
}

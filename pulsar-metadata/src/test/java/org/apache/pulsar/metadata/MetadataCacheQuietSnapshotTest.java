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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "metadata")
public class MetadataCacheQuietSnapshotTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final long REFRESH_MILLIS = 50;
    private static final Set<String> STAT_NAMES = Set.of("caffeine_cache_hit_total", "caffeine_cache_miss_total",
            "caffeine_cache_loads_total");
    private static final String PATH = "/quiet-snapshot";
    private LocalMemoryMetadataStore store;
    private MetadataCache<String> cache;
    private String cacheName;

    @BeforeMethod
    public void setUp() throws Exception {
        cacheName = "quiet-snapshot-" + UUID.randomUUID();
        store = spy(new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder()
                .metadataStoreName(cacheName).build()));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        try {
            if (store != null) {
                store.close();
            }
        } finally {
            if (cacheName != null) {
                // Remove only the metric fixtures created by this store; never clear the global registry.
                CacheMetricsCollector.CAFFEINE.removeCache(cacheName);
                CacheMetricsCollector.CAFFEINE.removeCache(cacheName + "-children");
                CacheMetricsCollector.CAFFEINE.removeCache(cacheName + "-exists");
            }
        }
    }

    private void createCache(long refreshMillis, long expiryMillis) {
        cache = store.getMetadataCache(String.class, MetadataCacheConfig.builder()
                .refreshAfterWriteMillis(refreshMillis).expireAfterWriteMillis(expiryMillis).build());
        assertThat(cache.supportsQuietSnapshots()).isTrue();
    }

    @Test
    public void testColdSnapshotDoesNotLoadOrRecordAMiss() {
        createCache(REFRESH_MILLIS, 60_000);
        assertQuietSnapshot(Optional.empty());
    }

    @Test(timeOut = 10_000)
    public void testPendingSnapshotDoesNotWaitOrStartAnotherLoad() throws Exception {
        createCache(REFRESH_MILLIS, 60_000);
        CompletableFuture<Optional<GetResult>> pending = new CompletableFuture<>();
        doReturn(pending).when(store).get(PATH);
        CompletableFuture<Optional<String>> load = cache.get(PATH);
        try {
            assertThat(load).isNotDone();
            assertQuietSnapshot(Optional.empty());
            assertThat(load).as("a quiet snapshot must not complete an unrelated load").isNotDone();
        } finally {
            pending.complete(result("ready"));
            load.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testFailedSnapshotDoesNotRetryLoad() {
        createCache(REFRESH_MILLIS, 60_000);
        doReturn(CompletableFuture.failedFuture(new IllegalStateException("load failed")))
                .when(store).get(PATH);
        assertThatThrownBy(() -> cache.get(PATH).join()).hasRootCauseMessage("load failed");
        awaitLoadCount(1);
        assertQuietSnapshot(Optional.empty());
    }

    @DataProvider
    public Object[][] storedValues() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "storedValues")
    public void testReadyAndNegativeSnapshotsDoNotChangeCacheStats(boolean present) throws Exception {
        createCache(60_000, 120_000);
        Optional<String> expected = present ? Optional.of("ready") : Optional.empty();
        doReturn(CompletableFuture.completedFuture(present ? result("ready") : Optional.empty()))
                .when(store).get(PATH);
        assertThat(cache.get(PATH).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isEqualTo(expected);
        awaitLoadCount(1);
        assertQuietSnapshot(expected);
    }

    @Test(dataProvider = "storedValues")
    public void testWarmStaleQuietSnapshotDoesNotReloadButLegacyLookupStillDoes(boolean present) throws Exception {
        createCache(REFRESH_MILLIS, 60_000);
        Optional<String> expected = present ? Optional.of("ready") : Optional.empty();
        doReturn(CompletableFuture.completedFuture(present ? result("ready") : Optional.empty()))
                .when(store).get(PATH);
        assertThat(cache.get(PATH).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isEqualTo(expected);
        awaitLoadCount(1);
        long loadedAt = System.nanoTime();
        CompletableFuture<Optional<GetResult>> reloading = new CompletableFuture<>();
        doReturn(reloading).when(store).get(PATH);
        try {
            await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(10))
                    .until(() -> System.nanoTime() - loadedAt >= Duration.ofMillis(REFRESH_MILLIS * 2).toNanos());
            assertQuietSnapshot(expected);
            // The existing getIfCached contract must remain untouched: no cold load, but stale hits may refresh.
            double hitsBeforeLegacyLookup = cacheStats().get("caffeine_cache_hit_total");
            assertThat(cache.getIfCached(PATH)).isEqualTo(expected);
            verify(store, times(1)).get(PATH);
            assertThat(cacheStats().get("caffeine_cache_hit_total")).isEqualTo(hitsBeforeLegacyLookup + 1);
            assertThat(reloading).as("refresh is pending, so lookup must return the old cached snapshot").isNotDone();
            assertQuietSnapshot(expected);
        } finally {
            reloading.complete(result("refreshed"));
        }
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(cache.getIfCachedQuietly(PATH)).contains("refreshed"));
    }

    @Test
    public void testExpiredSnapshotDoesNotLoadOrRefresh() throws Exception {
        createCache(0, REFRESH_MILLIS);
        doReturn(CompletableFuture.completedFuture(result("ready"))).when(store).get(PATH);
        assertThat(cache.get(PATH).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).contains("ready");
        awaitLoadCount(1);
        long loadedAt = System.nanoTime();
        await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(10))
                .until(() -> System.nanoTime() - loadedAt >= Duration.ofMillis(REFRESH_MILLIS * 2).toNanos());
        assertQuietSnapshot(Optional.empty());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUnsupportedQuietSnapshotDoesNotFallBackToLegacyLookup() {
        MetadataCache<String> unsupported = mock(MetadataCache.class, CALLS_REAL_METHODS);
        assertThat(unsupported.supportsQuietSnapshots()).isFalse();
        assertThat(unsupported.getIfCachedQuietly(PATH)).isEmpty();
        verify(unsupported, never()).getIfCached(anyString());
        verify(unsupported, never()).get(anyString());
    }

    private void assertQuietSnapshot(Optional<String> expected) {
        Map<String, Double> before = cacheStats();
        clearInvocations(store);
        for (int i = 0; i < 3; i++) {
            assertThat(cache.getIfCachedQuietly(PATH)).isEqualTo(expected);
        }
        assertThat(cacheStats()).as("quiet snapshots do not change Caffeine hit, miss or load counters")
                .isEqualTo(before);
        verifyNoInteractions(store);
    }

    private void awaitLoadCount(long count) {
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(cacheStats().get("caffeine_cache_loads_total")).isEqualTo((double) count));
    }

    private Map<String, Double> cacheStats() {
        Map<String, Double> result = new HashMap<>();
        CacheMetricsCollector.CAFFEINE.collect().forEach(family -> family.samples.forEach(sample -> {
            if (STAT_NAMES.contains(sample.name) && sample.labelValues.contains(cacheName)) {
                result.put(sample.name, sample.value);
            }
        }));
        assertThat(result.keySet()).containsExactlyInAnyOrderElementsOf(STAT_NAMES);
        return result;
    }

    private static Optional<GetResult> result(String value) {
        return Optional.of(new GetResult(('"' + value + '"').getBytes(UTF_8),
                new Stat(PATH, 0, 1, 1, false, true)));
    }
}

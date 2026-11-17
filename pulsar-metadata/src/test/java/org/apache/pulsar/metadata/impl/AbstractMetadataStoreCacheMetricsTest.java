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
package org.apache.pulsar.metadata.impl;

import static org.assertj.core.api.Assertions.assertThat;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that closing a metadata store unregisters the Caffeine metrics of its caches.
 */
@Test(groups = "metadata")
public class AbstractMetadataStoreCacheMetricsTest {
    private String storeName;
    private String childrenCacheName;
    private String existsCacheName;

    @BeforeMethod
    public void setUp() {
        storeName = "cache-metrics-" + UUID.randomUUID();
        childrenCacheName = storeName + "-children";
        existsCacheName = storeName + "-exists";
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        // Remove only the metric fixtures created by this test; never clear the global registry.
        CacheMetricsCollector.CAFFEINE.removeCache(childrenCacheName);
        CacheMetricsCollector.CAFFEINE.removeCache(existsCacheName);
        CacheMetricsCollector.CAFFEINE.removeCache(storeName);
    }

    @Test
    public void testCloseRemovesChildrenAndExistsCacheMetrics() throws Exception {
        LocalMemoryMetadataStore store = newStore();
        try {
            assertThat(exposedCacheNames()).as("cache series exposed while the store is open")
                    .contains(childrenCacheName, existsCacheName);
        } finally {
            store.close();
        }

        assertThat(exposedCacheNames()).as("cache series exposed after the store is closed")
                .doesNotContain(childrenCacheName, existsCacheName);

        // A second close must stay a no-op.
        store.close();
        assertThat(exposedCacheNames()).as("cache series exposed after closing the store twice")
                .doesNotContain(childrenCacheName, existsCacheName);
    }

    @Test
    public void testCloseKeepsMetricsOfLiveStoreWithSameName() throws Exception {
        LocalMemoryMetadataStore oldStore = newStore();
        // The newer store registers its caches under the same names, replacing the old store's caches.
        LocalMemoryMetadataStore liveStore = newStore();
        try {
            oldStore.close();
            assertThat(exposedCacheNames()).as("cache series of the live store after closing the old store")
                    .contains(childrenCacheName, existsCacheName);
        } finally {
            liveStore.close();
        }

        assertThat(exposedCacheNames()).as("cache series exposed after both stores are closed")
                .doesNotContain(childrenCacheName, existsCacheName);
    }

    @Test
    public void testCloseRemovesMetadataCacheMetrics() throws Exception {
        LocalMemoryMetadataStore store = newStore();
        try {
            // A metadata cache registers under the store name, and its user does not have to close it.
            store.getMetadataCache(String.class);
            assertThat(exposedCacheNames()).as("metadata cache series exposed while the store is open")
                    .contains(storeName);
        } finally {
            store.close();
        }

        assertThat(exposedCacheNames()).as("metadata cache series exposed after the store is closed")
                .doesNotContain(storeName);
    }

    @Test
    public void testCloseKeepsMetadataCacheMetricsOfLiveStoreWithSameName() throws Exception {
        LocalMemoryMetadataStore oldStore = newStore();
        oldStore.getMetadataCache(String.class);
        LocalMemoryMetadataStore liveStore = newStore();
        try {
            // The newer store's cache registers under the same name, replacing the old store's cache.
            liveStore.getMetadataCache(String.class);
            oldStore.close();
            assertThat(exposedCacheNames())
                    .as("metadata cache series of the live store after closing the old store")
                    .contains(storeName);
        } finally {
            liveStore.close();
        }

        assertThat(exposedCacheNames()).as("metadata cache series exposed after both stores are closed")
                .doesNotContain(storeName);
    }

    private LocalMemoryMetadataStore newStore() throws Exception {
        return new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().metadataStoreName(storeName).build());
    }

    private static Set<String> exposedCacheNames() {
        Set<String> cacheNames = new HashSet<>();
        for (MetricSnapshot snapshot : PrometheusRegistry.defaultRegistry.scrape()) {
            if (!snapshot.getMetadata().getPrometheusName().startsWith("caffeine_cache_")) {
                continue;
            }
            for (DataPointSnapshot dataPoint : snapshot.getDataPoints()) {
                String cacheName = dataPoint.getLabels().get("cache");
                if (cacheName != null) {
                    cacheNames.add(cacheName);
                }
            }
        }
        return cacheNames;
    }
}

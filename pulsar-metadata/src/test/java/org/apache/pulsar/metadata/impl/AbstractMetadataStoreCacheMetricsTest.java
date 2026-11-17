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

@Test(groups = "metadata")
public class AbstractMetadataStoreCacheMetricsTest {
    private String storeName;
    private String childrenCacheName;
    private String existsCacheName;
    private String metadataCacheName;

    @BeforeMethod
    public void setUp() {
        storeName = "cache-metrics-" + UUID.randomUUID();
        childrenCacheName = storeName + "-children";
        existsCacheName = storeName + "-exists";
        metadataCacheName = storeName;
    }

    @AfterMethod(alwaysRun = true)
    public void removeCacheMetricsRegisteredByThisTest() {
        CacheMetricsCollector.CAFFEINE.removeCache(childrenCacheName);
        CacheMetricsCollector.CAFFEINE.removeCache(existsCacheName);
        CacheMetricsCollector.CAFFEINE.removeCache(metadataCacheName);
    }

    @Test
    public void testCloseRemovesChildrenAndExistsCacheMetrics() throws Exception {
        LocalMemoryMetadataStore store = newStoreNamed(storeName);
        try {
            assertThat(exposedCacheNames()).as("cache series exposed while the store is open")
                    .contains(childrenCacheName, existsCacheName);
        } finally {
            store.close();
        }

        assertThat(exposedCacheNames()).as("cache series exposed after the store is closed")
                .doesNotContain(childrenCacheName, existsCacheName);

        store.close();
        assertThat(exposedCacheNames()).as("cache series exposed after closing the store twice")
                .doesNotContain(childrenCacheName, existsCacheName);
    }

    @Test
    public void testCloseKeepsMetricsOfLiveStoreWithSameName() throws Exception {
        LocalMemoryMetadataStore oldStore = newStoreNamed(storeName);
        LocalMemoryMetadataStore liveStore = newStoreNamed(storeName);
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
        LocalMemoryMetadataStore store = newStoreNamed(storeName);
        try {
            store.getMetadataCache(String.class);
            assertThat(exposedCacheNames()).as("metadata cache series exposed while the store is open")
                    .contains(metadataCacheName);
        } finally {
            store.close();
        }

        assertThat(exposedCacheNames()).as("metadata cache series exposed after the store is closed")
                .doesNotContain(metadataCacheName);
    }

    @Test
    public void testCloseKeepsMetadataCacheMetricsOfLiveStoreWithSameName() throws Exception {
        LocalMemoryMetadataStore oldStore = newStoreNamed(storeName);
        oldStore.getMetadataCache(String.class);
        LocalMemoryMetadataStore liveStore = newStoreNamed(storeName);
        try {
            liveStore.getMetadataCache(String.class);
            oldStore.close();
            assertThat(exposedCacheNames())
                    .as("metadata cache series of the live store after closing the old store")
                    .contains(metadataCacheName);
        } finally {
            liveStore.close();
        }

        assertThat(exposedCacheNames()).as("metadata cache series exposed after both stores are closed")
                .doesNotContain(metadataCacheName);
    }

    @Test
    public void testMetadataCacheCreatedAfterCloseLeavesNoMetrics() throws Exception {
        LocalMemoryMetadataStore closedStore = newStoreNamed(storeName);
        closedStore.close();

        closedStore.getMetadataCache(String.class);
        assertThat(exposedCacheNames()).as("series of a metadata cache created after the store is closed")
                .doesNotContain(metadataCacheName);
    }

    private static LocalMemoryMetadataStore newStoreNamed(String name) throws Exception {
        return new LocalMemoryMetadataStore("memory:local",
                MetadataStoreConfig.builder().metadataStoreName(name).build());
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

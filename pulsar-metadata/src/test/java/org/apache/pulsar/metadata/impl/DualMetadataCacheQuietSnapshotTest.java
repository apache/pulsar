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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.util.Optional;
import java.util.UUID;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataCacheConfig;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "metadata")
public class DualMetadataCacheQuietSnapshotTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testQuietSnapshotDelegatesToCurrentCacheWithoutLegacyLookup() throws Exception {
        String storeName = "dual-quiet-" + UUID.randomUUID();
        var storeConfig = MetadataStoreConfig.builder().metadataStoreName(storeName).build();
        var source = spy(new LocalMemoryMetadataStore("memory:local", storeConfig));
        DualMetadataStore dual;
        try {
            dual = new DualMetadataStore(source, storeConfig);
        } catch (Exception e) {
            source.close();
            throw e;
        }
        try (dual) {
            MetadataCache<String> sourceCache = mock(MetadataCache.class);
            MetadataCache<String> targetCache = mock(MetadataCache.class);
            var config = MetadataCacheConfig.builder().build();
            doReturn(sourceCache).when(source).getMetadataCache(String.class, config);
            when(sourceCache.supportsQuietSnapshots()).thenReturn(true);
            when(targetCache.supportsQuietSnapshots()).thenReturn(false);
            when(sourceCache.getIfCachedQuietly("/path")).thenReturn(Optional.of("source"));
            when(targetCache.getIfCachedQuietly("/path")).thenReturn(Optional.empty());
            var cache = new DualMetadataCache<>(dual, String.class, null, "quiet-test", null, config);

            assertThat(cache.supportsQuietSnapshots()).isTrue();
            assertThat(cache.getIfCachedQuietly("/path")).contains("source");
            cache.getMetadataCache().set(targetCache);
            assertThat(cache.supportsQuietSnapshots()).isFalse();
            assertThat(cache.getIfCachedQuietly("/path")).isEmpty();
            verify(sourceCache).supportsQuietSnapshots();
            verify(sourceCache).getIfCachedQuietly("/path");
            verify(targetCache).supportsQuietSnapshots();
            verify(targetCache).getIfCachedQuietly("/path");
            verifyNoMoreInteractions(sourceCache, targetCache);
        } finally {
            CacheMetricsCollector.CAFFEINE.removeCache(storeName);
            CacheMetricsCollector.CAFFEINE.removeCache(storeName + "-children");
            CacheMetricsCollector.CAFFEINE.removeCache(storeName + "-exists");
        }
    }
}

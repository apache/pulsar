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
package org.apache.pulsar.common.naming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

public class TopicNameCacheTest {

    @Test
    public void shrinkCache() {
        // Test that the cache can shrink when the size exceeds the maximum limit
        TopicNameCache cache = TopicNameCache.INSTANCE;
        for (int i = 0; i < TopicNameCache.cacheMaxSize; i++) {
            cache.get("persistent://tenant/namespace/topic" + i);
        }

        // check that the cache size is at maximum
        assertEquals(cache.size(), TopicNameCache.cacheMaxSize);

        // Add one more topic to trigger the cache shrink
        cache.get("persistent://tenant/namespace/topic100101");

        // The cache should have reduced its size by the configured percentage
        assertThat(cache.size()).isEqualTo(
                        (int) (TopicNameCache.cacheMaxSize * ((100 - TopicNameCache.reduceSizeByPercentage) / 100.0)))
                .as("Cache size should be reduced after adding an extra topic beyond the max size");
    }

    @Test
    public void softReferenceHandling() {
        int defaultCacheMaxSize = TopicNameCache.cacheMaxSize;
        long defaultCacheMaintenceTaskIntervalMillis = TopicNameCache.cacheMaintenanceTaskIntervalMillis;
        try {
            TopicNameCache.cacheMaxSize = Integer.MAX_VALUE;
            TopicNameCache.cacheMaintenanceTaskIntervalMillis = 10L;

            TopicNameCache cache = TopicNameCache.INSTANCE;
            for (int i = 0; i < 2_000_000; i++) {
                cache.get("persistent://tenant/namespace/topic" + RandomStringUtils.randomAlphabetic(100));
                if (i % 100_000 == 0) {
                    System.out.println(i + " topics added to cache. Current size: " + cache.size());
                }
            }
        } finally {
            // Reset the cache settings to default after the test
            TopicNameCache.cacheMaxSize = defaultCacheMaxSize;
            TopicNameCache.cacheMaintenanceTaskIntervalMillis = defaultCacheMaintenceTaskIntervalMillis;
        }
    }
}
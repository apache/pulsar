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
package org.apache.pulsar.common.stats;

import static org.testng.Assert.assertEquals;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.prometheus.client.Collector;
import java.util.List;
import org.testng.annotations.Test;

public class CacheMetricsCollectorTest {

    @Test
    public void testCache() {
        AsyncCache<String, Long> cache = Caffeine.newBuilder().recordStats().buildAsync();
        CacheMetricsCollector.CAFFEINE.addCache("testcache", cache);
        cache.get("key1", key -> 1L);
        final List<Collector.MetricFamilySamples> collect = CacheMetricsCollector.CAFFEINE.collect();
        for (Collector.MetricFamilySamples samples : collect) {
            if (samples.name.equals("caffeine_cache_estimated_size")) {
                assertEquals(samples.samples.size(), 1);
            }
            if (samples.name.equals("caffeine_cache_eviction_weight")) {
                for (Collector.MetricFamilySamples.Sample sample : samples.samples) {
                    if ("caffeine_cache_eviction_weight".equalsIgnoreCase(sample.name)) {
                        assertEquals(sample.value, 0.0);
                    }
                }
            }
        }
    }
}

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

import static org.assertj.core.api.Assertions.assertThat;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class CacheMetricsCollectorTest {

    @Test
    public void testRemovingReplacedCachePreservesCurrentMetrics() {
        AsyncCache<String, Long> oldCache = Caffeine.newBuilder().recordStats().buildAsync();
        AsyncCache<String, Long> currentCache = Caffeine.newBuilder().recordStats().buildAsync();
        String name = "replaced-cache";
        try {
            CacheMetricsCollector.CAFFEINE.addCache(name, oldCache);
            CacheMetricsCollector.CAFFEINE.addCache(name, currentCache);
            CacheMetricsCollector.removeCache(name, oldCache);
            assertThat(samples(name)).containsKey("caffeine_cache_estimated_size");
            CacheMetricsCollector.removeCache(name, currentCache);
            CacheMetricsCollector.removeCache(name, currentCache);
            assertThat(samples(name)).isEmpty();
        } finally {
            CacheMetricsCollector.CAFFEINE.removeCache(name);
        }
    }

    @Test
    public void testLoadingCacheKeepsLegacyMetricNamesAndTypes() throws IOException {
        LoadingCache<String, Long> cache = Caffeine.newBuilder().recordStats().build(key -> 1L);
        String name = "loading-cache";
        try {
            CacheMetricsCollector.CAFFEINE.addCache(name, cache);
            cache.get("key1");
            cache.get("key1");

            Map<String, Double> samples = samples(name);
            assertThat(samples).containsEntry("caffeine_cache_hit", 1.0)
                    .containsEntry("caffeine_cache_miss", 1.0)
                    .containsEntry("caffeine_cache_requests", 2.0)
                    .containsEntry("caffeine_cache_eviction", 0.0)
                    .containsEntry("caffeine_cache_eviction_weight", 0.0)
                    .containsEntry("caffeine_cache_load_failure", 0.0)
                    .containsEntry("caffeine_cache_loads", 1.0)
                    .containsEntry("caffeine_cache_estimated_size", 1.0)
                    .containsKey("caffeine_cache_load_duration_seconds")
                    .doesNotContainKey("caffeine_cache_weighted_size");

            // The text exposition keeps the simpleclient names: counters get _total, the weight stays a gauge.
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrometheusTextFormatWriter.create().write(out,
                    PrometheusRegistry.defaultRegistry.scrape(n -> n.startsWith("caffeine_cache")));
            String text = out.toString(StandardCharsets.UTF_8);
            assertThat(text).contains("# TYPE caffeine_cache_hit_total counter\n")
                    .contains("caffeine_cache_hit_total{cache=\"" + name + "\"} 1.0\n")
                    .contains("# TYPE caffeine_cache_eviction_weight gauge\n")
                    .contains("caffeine_cache_eviction_weight{cache=\"" + name + "\"} 0.0\n")
                    .contains("# TYPE caffeine_cache_load_duration_seconds summary\n")
                    .contains("caffeine_cache_load_duration_seconds_count{cache=\"" + name + "\"} 1\n");
        } finally {
            CacheMetricsCollector.CAFFEINE.removeCache(name);
        }
    }

    /** Values of every family that has a data point for the given cache, keyed by the family's base name. */
    private static Map<String, Double> samples(String cacheName) {
        Map<String, Double> result = new HashMap<>();
        for (MetricSnapshot snapshot : CacheMetricsCollector.CAFFEINE.collect()) {
            for (DataPointSnapshot dataPoint : snapshot.getDataPoints()) {
                if (cacheName.equals(dataPoint.getLabels().get("cache"))) {
                    result.put(snapshot.getMetadata().getPrometheusName(), value(dataPoint));
                }
            }
        }
        return result;
    }

    private static double value(DataPointSnapshot dataPoint) {
        if (dataPoint instanceof CounterSnapshot.CounterDataPointSnapshot c) {
            return c.getValue();
        }
        if (dataPoint instanceof GaugeSnapshot.GaugeDataPointSnapshot g) {
            return g.getValue();
        }
        if (dataPoint instanceof SummarySnapshot.SummaryDataPointSnapshot s) {
            return s.getCount();
        }
        throw new IllegalArgumentException(dataPoint.getClass().getName());
    }
}

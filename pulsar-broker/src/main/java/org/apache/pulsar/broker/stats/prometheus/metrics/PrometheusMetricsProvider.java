/**
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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.prometheus.client.Collector;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * A <i>Prometheus</i> based {@link PrometheusRawMetricsProvider} implementation.
 */
public class PrometheusMetricsProvider implements StatsProvider, PrometheusRawMetricsProvider {
    private ScheduledExecutorService executor;

    public static final String PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = "prometheusStatsLatencyRolloverSeconds";
    public static final int DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = 60;
    public static final String CLUSTER_NAME = "cluster";
    public static final String DEFAULT_CLUSTER_NAME = "pulsar";

    private Map<String, String> labels;
    private final CachingStatsProvider cachingStatsProvider;

    /**
     * These acts a registry of the metrics defined in this provider.
     */
    // The outside map is used to group the same metrics name,
    // but with different labels metrics to ensure all the metrics with same metric name are grouped.
    // `https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/
    // exposition_formats.md#grouping-and-sorting`
    public final ConcurrentMap<String,
        ConcurrentMap<ScopeContext, LongAdderCounter>> counters = new ConcurrentHashMap<>();
    public final ConcurrentMap<String,
        ConcurrentMap<ScopeContext, SimpleGauge<? extends Number>>> gauges = new ConcurrentHashMap<>();
    public final ConcurrentMap<ScopeContext, DataSketchesOpStatsLogger> opStats = new ConcurrentHashMap<>();

    final ConcurrentMap<ScopeContext, ThreadScopedDataSketchesStatsLogger> threadScopedOpStats =
        new ConcurrentHashMap<>();
    final ConcurrentMap<ScopeContext, ThreadScopedLongAdderCounter> threadScopedCounters =
        new ConcurrentHashMap<>();

    public PrometheusMetricsProvider() {
        this.cachingStatsProvider = new CachingStatsProvider(new StatsProvider() {
            @Override
            public void start(Configuration conf) {
                // nop
            }

            @Override
            public void stop() {
                // nop
            }

            @Override
            public StatsLogger getStatsLogger(String scope) {
                return new PrometheusStatsLogger(PrometheusMetricsProvider.this, scope, labels);
            }

            @Override
            public String getStatsName(String... statsComponents) {
                String completeName;
                if (statsComponents.length == 0) {
                    return "";
                } else if (statsComponents[0].isEmpty()) {
                    completeName = StringUtils.join(statsComponents, '_', 1, statsComponents.length);
                } else {
                    completeName = StringUtils.join(statsComponents, '_');
                }
                return Collector.sanitizeMetricName(completeName);
            }
        });
    }

    public void start(Configuration conf) {
        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("metrics"));
        int latencyRolloverSeconds = conf.getInt(PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS);
        labels = Collections.singletonMap(CLUSTER_NAME, conf.getString(CLUSTER_NAME, DEFAULT_CLUSTER_NAME));

        executor.scheduleAtFixedRate(catchingAndLoggingThrowables(this::rotateLatencyCollection),
            1, latencyRolloverSeconds, TimeUnit.SECONDS);

    }

    public void stop() {
        executor.shutdown();
    }

    public StatsLogger getStatsLogger(String scope) {
        return cachingStatsProvider.getStatsLogger(scope);
    }

    @Override
    public void writeAllMetrics(Writer writer) throws IOException {
        throw new UnsupportedOperationException("writeAllMetrics is not support yet");
    }

    @Override
    public void generate(SimpleTextOutputStream writer) {
        PrometheusTextFormat prometheusTextFormat = new PrometheusTextFormat();
        gauges.forEach((name, metric) ->
            metric.forEach((sc, gauge) -> prometheusTextFormat.writeGauge(writer, sc.getScope(), gauge)));
        counters.forEach((name, metric) ->
            metric.forEach((sc, counter) -> prometheusTextFormat.writeCounter(writer, sc.getScope(), counter)));
        opStats.forEach((sc, opStatLogger) ->
                prometheusTextFormat.writeOpStat(writer, sc.getScope(), opStatLogger));
    }

    public String getStatsName(String... statsComponents) {
        return cachingStatsProvider.getStatsName(statsComponents);
    }

    @VisibleForTesting
    void rotateLatencyCollection() {
        opStats.forEach((name, metric) -> {
            metric.rotateLatencyCollection();
        });
    }
}

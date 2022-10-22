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
import io.prometheus.client.Collector;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.CachingStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.util.ExecutorProvider;

/**
 * A <i>Prometheus</i> based {@link StatsProvider} implementation.
 */
public class PrometheusMetricsProvider implements StatsProvider {
    private ScheduledExecutorService executor;

    public static final String PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = "prometheusStatsLatencyRolloverSeconds";
    public static final int DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = 60;
    public static final String CLUSTER_NAME = "cluster";
    public static final String DEFAULT_CLUSTER_NAME = "pulsar";

    private String cluster;
    private final CachingStatsProvider cachingStatsProvider;

    /**
     * These acts a registry of the metrics defined in this provider.
     */
    public final ConcurrentMap<String, LongAdderCounter> counters = new ConcurrentSkipListMap<>();
    public final ConcurrentMap<String, SimpleGauge<? extends Number>> gauges = new ConcurrentSkipListMap<>();
    public final ConcurrentMap<String, DataSketchesOpStatsLogger> opStats = new ConcurrentSkipListMap<>();

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
                return new PrometheusStatsLogger(PrometheusMetricsProvider.this, scope);
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

    @Override
    public void start(Configuration conf) {
        executor = Executors.newSingleThreadScheduledExecutor(
                new ExecutorProvider.ExtendedThreadFactory("metrics"));

        int latencyRolloverSeconds = conf.getInt(PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS);
        cluster = conf.getString(CLUSTER_NAME, DEFAULT_CLUSTER_NAME);

        executor.scheduleAtFixedRate(catchingAndLoggingThrowables(this::rotateLatencyCollection),
                1, latencyRolloverSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        executor.shutdownNow();
    }

    @Override
    public StatsLogger getStatsLogger(String scope) {
        return this.cachingStatsProvider.getStatsLogger(scope);
    }

    @Override
    public void writeAllMetrics(Writer writer) throws IOException {
        gauges.forEach((name, gauge) -> PrometheusTextFormatUtil.writeGauge(writer, name, cluster, gauge));
        counters.forEach((name, counter) -> PrometheusTextFormatUtil.writeCounter(writer, name, cluster, counter));
        opStats.forEach((name, opStatLogger) -> PrometheusTextFormatUtil.writeOpStat(writer, name, cluster,
                opStatLogger));
    }

    @Override
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

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

import com.google.common.base.Joiner;
import io.prometheus.client.Collector;
import java.util.Map;
import java.util.TreeMap;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@code Prometheus} based {@link StatsLogger} implementation.
 */
public class PrometheusStatsLogger implements StatsLogger {

    private final PrometheusMetricsProvider provider;
    private final String scope;
    private final Map<String, String> labels;

    PrometheusStatsLogger(PrometheusMetricsProvider provider, String scope, Map<String, String> labels) {
        this.provider = provider;
        this.scope = scope;
        this.labels = labels;
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        return provider.opStats.computeIfAbsent(scopeContext(name), x -> new DataSketchesOpStatsLogger(labels));
    }

    public OpStatsLogger getThreadScopedOpStatsLogger(String name) {
        return provider.threadScopedOpStats.computeIfAbsent(scopeContext(name),
                x -> new ThreadScopedDataSketchesStatsLogger(provider, x, labels));
    }

    @Override
    public Counter getCounter(String name) {
        return provider.counters.computeIfAbsent(scopeContext(name), x -> new LongAdderCounter(labels));
    }

    public Counter getThreadScopedCounter(String name) {
        return provider.threadScopedCounters.computeIfAbsent(scopeContext(name),
                x -> new ThreadScopedLongAdderCounter(provider, x, labels));
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        provider.gauges.computeIfAbsent(scopeContext(name), x -> new SimpleGauge<T>(gauge, labels));
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        // no-op
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // no-op
    }

    @Override
    public StatsLogger scope(String name) {
        return new PrometheusStatsLogger(provider, completeName(name), labels);
    }

    @Override
    public StatsLogger scopeLabel(String labelName, String labelValue) {
        Map<String, String> newLabels = new TreeMap<>(labels);
        newLabels.put(labelName, labelValue);
        return new PrometheusStatsLogger(provider, scope, newLabels);
    }

    private ScopeContext scopeContext(String name) {
        return new ScopeContext(completeName(name), labels);
    }

    private String completeName(String name) {
        return Collector.sanitizeMetricName(scope.isEmpty() ? name : Joiner.on('_').join(scope, name));
    }
}

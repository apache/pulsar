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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@code StatsLogger} that caches the stats objects created by other {@code StatsLogger}.
 */
public class CachingStatsLogger implements StatsLogger {

    protected final StatsLogger underlying;
    protected final ConcurrentMap<ScopeContext, Counter> counters;
    protected final ConcurrentMap<ScopeContext, OpStatsLogger> opStatsLoggers;
    protected final ConcurrentMap<ScopeContext, StatsLogger> scopeStatsLoggers;
    protected final ConcurrentMap<ScopeContext, StatsLogger> scopeLabelStatsLoggers;

    private final String scope;
    private final Map<String, String> labels;

    public CachingStatsLogger(String scope, StatsLogger statsLogger, Map<String, String> labels) {
        this.scope = scope;
        this.labels = labels;
        this.underlying = statsLogger;
        this.counters = new ConcurrentHashMap<>();
        this.opStatsLoggers = new ConcurrentHashMap<>();
        this.scopeStatsLoggers = new ConcurrentHashMap<>();
        this.scopeLabelStatsLoggers = new ConcurrentHashMap<>();
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CachingStatsLogger)) {
            return false;
        }
        CachingStatsLogger another = (CachingStatsLogger) obj;
        return underlying.equals(another.underlying);
    }

    @Override
    public String toString() {
        return underlying.toString();
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        return opStatsLoggers.computeIfAbsent(scopeContext(name), x -> underlying.getOpStatsLogger(name));
    }

    @Override
    public Counter getCounter(String name) {
        return counters.computeIfAbsent(scopeContext(name), x -> underlying.getCounter(name));
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        underlying.registerGauge(name, gauge);
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        underlying.unregisterGauge(name, gauge);
    }

    @Override
    public StatsLogger scope(String name) {
        return scopeStatsLoggers.computeIfAbsent(scopeContext(name),
            x -> new CachingStatsLogger(scope, underlying.scope(name), labels));
    }

    @Override
    public StatsLogger scopeLabel(String labelName, String labelValue) {
        Map<String, String> newLabels = new TreeMap<>(labels);
        newLabels.put(labelName, labelValue);
        return scopeLabelStatsLoggers.computeIfAbsent(new ScopeContext(completeName(""), newLabels),
            x -> new CachingStatsLogger(scope, underlying.scopeLabel(labelName, labelValue), newLabels));
    }


    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        scopeStatsLoggers.remove(scopeContext(name), statsLogger);
    }

    /**
     Thread-scoped stats not currently supported.
     */
    @Override
    public OpStatsLogger getThreadScopedOpStatsLogger(String name) {
        return getOpStatsLogger(name);
    }

    /**
     Thread-scoped stats not currently supported.
     */
    @Override
    public Counter getThreadScopedCounter(String name) {
        return getCounter(name);
    }

    private ScopeContext scopeContext(String name) {
        return new ScopeContext(completeName(name), labels);
    }

    private String completeName(String name) {
        return Collector.sanitizeMetricName(scope.isEmpty() ? name : Joiner.on('_').join(scope, name));
    }
}

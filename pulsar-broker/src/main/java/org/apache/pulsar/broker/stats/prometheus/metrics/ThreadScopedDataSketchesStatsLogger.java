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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * OpStatsLogger implementation that lazily registers OpStatsLoggers per thread
 * with added labels for the threadpool/thresd name and thread no.
 */
public class ThreadScopedDataSketchesStatsLogger implements OpStatsLogger {

    private ThreadLocal<DataSketchesOpStatsLogger> statsLoggers;
    private DataSketchesOpStatsLogger defaultStatsLogger;
    private Map<String, String> originalLabels;
    private ScopeContext scopeContext;
    private PrometheusMetricsProvider provider;

    public ThreadScopedDataSketchesStatsLogger(PrometheusMetricsProvider provider,
                                               ScopeContext scopeContext,
                                               Map<String, String> labels) {
        this.provider = provider;
        this.scopeContext = scopeContext;
        this.originalLabels = labels;
        this.defaultStatsLogger = new DataSketchesOpStatsLogger(labels);

        Map<String, String> defaultLabels = new HashMap<>(labels);
        defaultLabels.put("threadPool", "?");
        defaultLabels.put("thread", "?");
        this.defaultStatsLogger.initializeThread(defaultLabels);

        this.statsLoggers = ThreadLocal.withInitial(() -> {
            return new DataSketchesOpStatsLogger(labels);
        });
    }

    @Override
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        getStatsLogger().registerFailedEvent(eventLatency, unit);
    }

    @Override
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        getStatsLogger().registerSuccessfulEvent(eventLatency, unit);
    }

    @Override
    public void registerSuccessfulValue(long value) {
        getStatsLogger().registerSuccessfulValue(value);
    }

    @Override
    public void registerFailedValue(long value) {
        getStatsLogger().registerFailedValue(value);
    }

    @Override
    public OpStatsData toOpStatsData() {
        // Not relevant as we don't use JMX here
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        // Not relevant as we don't use JMX here
        throw new UnsupportedOperationException();
    }

    private DataSketchesOpStatsLogger getStatsLogger() {
        DataSketchesOpStatsLogger statsLogger = statsLoggers.get();

        // Lazy registration
        // Update the stats logger with the thread labels then add to the provider
        // If for some reason this thread did not get registered,
        // then we fallback to a standard OpsStatsLogger (defaultStatsLogger)
        if (!statsLogger.isThreadInitialized()) {
            ThreadRegistry.ThreadPoolThread tpt = ThreadRegistry.get();
            if (tpt == null) {
                statsLoggers.set(defaultStatsLogger);
                provider.opStats.put(new ScopeContext(scopeContext.getScope(), originalLabels), defaultStatsLogger);
                return defaultStatsLogger;
            } else {
                Map<String, String> threadScopedlabels = new HashMap<>(originalLabels);
                threadScopedlabels.put("threadPool", tpt.getThreadPool());
                threadScopedlabels.put("thread", String.valueOf(tpt.getOrdinal()));

                statsLogger.initializeThread(threadScopedlabels);
                provider.opStats.put(new ScopeContext(scopeContext.getScope(), threadScopedlabels), statsLogger);
            }
        }

        return statsLogger;
    }
}
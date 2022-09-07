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
import org.apache.bookkeeper.stats.Counter;

/**
 * {@link Counter} implementation that lazily registers LongAdderCounters per thread
 *  * with added labels for the threadpool/thread name and thread no.
 */
public class ThreadScopedLongAdderCounter implements Counter {
    private ThreadLocal<LongAdderCounter> counters;
    private LongAdderCounter defaultCounter;
    private Map<String, String> originalLabels;
    private ScopeContext scopeContext;
    private PrometheusMetricsProvider provider;

    public ThreadScopedLongAdderCounter(PrometheusMetricsProvider provider,
                                        ScopeContext scopeContext,
                                        Map<String, String> labels) {
        this.provider = provider;
        this.scopeContext = scopeContext;
        this.originalLabels = new HashMap<>(labels);
        this.defaultCounter = new LongAdderCounter(labels);
        Map<String, String> defaultLabels = new HashMap<>(labels);
        defaultLabels.put("threadPool", "?");
        defaultLabels.put("thread", "?");
        this.defaultCounter.initializeThread(defaultLabels);

        this.counters = ThreadLocal.withInitial(() -> {
            return new LongAdderCounter(labels);
        });
    }

    @Override
    public void clear() {
        getCounter().clear();
    }

    @Override
    public void inc() {
        getCounter().inc();
    }

    @Override
    public void dec() {
        getCounter().dec();
    }

    @Override
    public void add(long delta) {
        getCounter().add(delta);
    }

    @Override
    public Long get() {
        return getCounter().get();
    }

    private LongAdderCounter getCounter() {
        LongAdderCounter counter = counters.get();

        // Lazy registration
        // Update the counter with the thread labels then add to the provider
        // If for some reason this thread did not get registered,
        // then we fallback to a standard counter (defaultCounter)
        if (!counter.isThreadInitialized()) {
            ThreadRegistry.ThreadPoolThread tpt = ThreadRegistry.get();

            if (tpt == null) {
                counters.set(defaultCounter);
                provider.counters.put(new ScopeContext(scopeContext.getScope(), originalLabels), defaultCounter);
                return defaultCounter;
            } else {
                Map<String, String> threadScopedlabels = new HashMap<>(originalLabels);
                threadScopedlabels.put("threadPool", tpt.getThreadPool());
                threadScopedlabels.put("thread", String.valueOf(tpt.getOrdinal()));

                counter.initializeThread(threadScopedlabels);
                provider.counters.put(new ScopeContext(scopeContext.getScope(), threadScopedlabels), counter);
            }
        }

        return counter;
    }
}

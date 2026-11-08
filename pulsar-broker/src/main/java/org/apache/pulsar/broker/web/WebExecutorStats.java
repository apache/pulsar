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
package org.apache.pulsar.broker.web;

import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.opentelemetry.annotations.PulsarDeprecatedMetric;

@Deprecated
class WebExecutorStats implements AutoCloseable {
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

    @PulsarDeprecatedMetric(newMetricName = WebExecutorThreadPoolStats.LIMIT_COUNTER)
    private final GaugeWithCallback maxThreads;
    @PulsarDeprecatedMetric(newMetricName = WebExecutorThreadPoolStats.LIMIT_COUNTER)
    private final GaugeWithCallback minThreads;
    @PulsarDeprecatedMetric(newMetricName = WebExecutorThreadPoolStats.USAGE_COUNTER)
    private final GaugeWithCallback idleThreads;
    @PulsarDeprecatedMetric(newMetricName = WebExecutorThreadPoolStats.USAGE_COUNTER)
    private final GaugeWithCallback activeThreads;
    @PulsarDeprecatedMetric(newMetricName = WebExecutorThreadPoolStats.USAGE_COUNTER)
    private final GaugeWithCallback currentThreads;

    private static volatile WebExecutorStats instance;

    static synchronized WebExecutorStats getStats(WebExecutorThreadPool executor) {
        if (null == instance) {
            instance = new WebExecutorStats(executor);
        }

        return instance;
    }

    private WebExecutorStats(WebExecutorThreadPool executor) {
        this.maxThreads = GaugeWithCallback.builder()
                .name("pulsar_web_executor_max_threads")
                .help("-")
                .callback(callback -> callback.call(executor.getMaxThreads()))
                .register();

        this.minThreads = GaugeWithCallback.builder()
                .name("pulsar_web_executor_min_threads")
                .help("-")
                .callback(callback -> callback.call(executor.getMinThreads()))
                .register();

        this.idleThreads = GaugeWithCallback.builder()
                .name("pulsar_web_executor_idle_threads")
                .help("-")
                .callback(callback -> callback.call(executor.getIdleThreads()))
                .register();

        this.activeThreads = GaugeWithCallback.builder()
                .name("pulsar_web_executor_active_threads")
                .help("-")
                .callback(callback -> callback.call(executor.getThreads() - executor.getIdleThreads()))
                .register();

        this.currentThreads = GaugeWithCallback.builder()
                .name("pulsar_web_executor_current_threads")
                .help("-")
                .callback(callback -> callback.call(executor.getThreads()))
                .register();
    }

    @Override
    public void close() throws Exception {
        if (CLOSED.compareAndSet(false, true)) {
            PrometheusRegistry.defaultRegistry.unregister(this.activeThreads);
            PrometheusRegistry.defaultRegistry.unregister(this.maxThreads);
            PrometheusRegistry.defaultRegistry.unregister(this.minThreads);
            PrometheusRegistry.defaultRegistry.unregister(this.idleThreads);
            PrometheusRegistry.defaultRegistry.unregister(this.currentThreads);
        }
    }
}

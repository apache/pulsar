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
package org.apache.pulsar.broker.stats.prometheus;

import static org.apache.pulsar.common.stats.JvmMetrics.getJvmDirectMemoryUsed;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.common.util.DirectMemoryUtils;

public class MetricsExports {
    private static boolean initialized = false;

    private MetricsExports() {
    }

    public static synchronized void initialize() {
        if (!initialized) {
            JvmMetrics.builder().register();
            register(PrometheusRegistry.defaultRegistry);
            initialized = true;
        }
    }

    public static void register(PrometheusRegistry registry) {
        GaugeWithCallback.builder()
                .name("jvm_memory_direct_bytes_used")
                .help("-")
                .callback(callback -> callback.call(getJvmDirectMemoryUsed()))
                .register(registry);

        GaugeWithCallback.builder()
                .name("jvm_memory_direct_bytes_max")
                .help("-")
                .callback(callback -> callback.call(DirectMemoryUtils.jvmMaxDirectMemory()))
                .register(registry);

        // metric to export pulsar version info. Nothing exports exemplars, so the gauge skips the sampling that
        // 1.x gauges do by default.
        Gauge.builder()
                .name("pulsar_version_info")
                .help("-")
                .labelNames("version", "commit")
                .withoutExemplars()
                .register(registry)
                .labelValues(PulsarVersion.getVersion(), PulsarVersion.getGitSha())
                .set(1);
    }
}

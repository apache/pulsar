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

import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.pulsar.PulsarVersion;

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
        NettyDirectMemoryGauges directMemoryGauges = null;
        if (registry == PrometheusRegistry.defaultRegistry) {
            // The broker exports the gauges for the life of the JVM, so this registration is closed only if this
            // call fails
            directMemoryGauges = NettyDirectMemoryGauges.register();
        } else {
            NettyDirectMemoryGauges.registerOn(registry);
        }

        try {
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
        } catch (RuntimeException e) {
            if (directMemoryGauges != null) {
                directMemoryGauges.close();
            }
            throw e;
        }
    }
}

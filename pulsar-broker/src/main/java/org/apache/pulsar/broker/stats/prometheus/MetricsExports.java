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
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.common.util.DirectMemoryUtils;

public class MetricsExports {
    private static boolean initialized = false;

    private MetricsExports() {
    }

    public static synchronized void initialize() {
        if (!initialized) {
            DefaultExports.initialize();
            register(CollectorRegistry.defaultRegistry);
            initialized = true;
        }
    }

    public static void register(CollectorRegistry registry) {
        Gauge.build("jvm_memory_direct_bytes_used", "-").create().setChild(new Gauge.Child() {
            @Override
            public double get() {
                return getJvmDirectMemoryUsed();
            }
        }).register(registry);

        Gauge.build("jvm_memory_direct_bytes_max", "-").create().setChild(new Gauge.Child() {
            @Override
            public double get() {
                return DirectMemoryUtils.jvmMaxDirectMemory();
            }
        }).register(registry);

        // metric to export pulsar version info
        Gauge.build("pulsar_version_info", "-")
                .labelNames("version", "commit").create()
                .setChild(new Gauge.Child() {
                    @Override
                    public double get() {
                        return 1.0;
                    }
                }, PulsarVersion.getVersion(), PulsarVersion.getGitSha())
                .register(registry);
    }
}

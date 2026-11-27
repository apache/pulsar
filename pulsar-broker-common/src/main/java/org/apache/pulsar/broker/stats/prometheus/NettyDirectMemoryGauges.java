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

import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import lombok.CustomLog;
import org.apache.pulsar.common.stats.JvmMetrics;
import org.apache.pulsar.common.util.DirectMemoryUtils;

/**
 * The {@code jvm_memory_direct_bytes_used} and {@code jvm_memory_direct_bytes_max} gauges, which report the direct
 * memory that Netty counts. The broker, the proxy and the standalone functions worker register them through
 * {@link #register()} on {@link PrometheusRegistry#defaultRegistry}, which is per JVM. The gauges are in the registry
 * while at least one of the instances that {@link #register()} returned is open.
 */
@CustomLog
public final class NettyDirectMemoryGauges implements AutoCloseable {

    // Built once and registered again after the last instance has closed. With a new gauge of the same name, a scrape
    // that runs meanwhile could get the name twice: PrometheusRegistry.register() adds a gauge before it checks the
    // name, and a scrape can get both a removed gauge and the one that replaced it.
    private static final List<GaugeWithCallback> GAUGES = List.of(
            gauge("jvm_memory_direct_bytes_used", JvmMetrics::getJvmDirectMemoryUsed),
            gauge("jvm_memory_direct_bytes_max", DirectMemoryUtils::jvmMaxDirectMemory));

    // The first instance to open registers the gauges, and the last one to close unregisters them. Guarded by the
    // class lock.
    private static int openInstances;
    private static final List<GaugeWithCallback> REGISTERED_GAUGES = new ArrayList<>(GAUGES.size());

    // Guarded by the class lock
    private boolean closed;

    private NettyDirectMemoryGauges() {
    }

    /**
     * Registers the gauges, unless an instance that an earlier call returned is still open.
     *
     * @return what unregisters the gauges on {@link #close()}, unless other instances are still open then
     */
    public static synchronized NettyDirectMemoryGauges register() {
        if (openInstances++ == 0) {
            for (GaugeWithCallback gauge : GAUGES) {
                try {
                    PrometheusRegistry.defaultRegistry.register(gauge);
                    REGISTERED_GAUGES.add(gauge);
                } catch (IllegalArgumentException nameIsTaken) {
                    // Code that does not use this class has registered the name
                    log.debug().exceptionMessage(nameIsTaken).log("Prometheus gauge is already registered");
                }
            }
        }
        return new NettyDirectMemoryGauges();
    }

    private static GaugeWithCallback gauge(String name, LongSupplier currentValue) {
        return GaugeWithCallback.builder()
                .name(name)
                .help("-")
                .callback(callback -> callback.call(currentValue.getAsLong()))
                .build();
    }

    @Override
    public void close() {
        synchronized (NettyDirectMemoryGauges.class) {
            if (closed) {
                return;
            }
            closed = true;
            if (--openInstances == 0) {
                REGISTERED_GAUGES.forEach(PrometheusRegistry.defaultRegistry::unregister);
                REGISTERED_GAUGES.clear();
            }
        }
    }
}

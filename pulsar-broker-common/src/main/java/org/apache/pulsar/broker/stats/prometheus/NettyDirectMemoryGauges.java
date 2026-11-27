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
import java.util.Map;
import java.util.function.LongSupplier;
import lombok.CustomLog;
import org.apache.pulsar.common.stats.JvmMetrics;
import org.apache.pulsar.common.util.DirectMemoryUtils;

/**
 * The {@code jvm_memory_direct_bytes_used} and {@code jvm_memory_direct_bytes_max} gauges, which report the direct
 * memory that Netty counts. The broker, the proxy and the standalone functions worker register them through
 * {@link #register()} on {@link PrometheusRegistry#defaultRegistry}, which is per JVM. The gauges are in that registry
 * while at least one of the instances that {@link #register()} returned is open, except for a name that code outside
 * this class registered first: that name stays with that code.
 */
@CustomLog
public final class NettyDirectMemoryGauges implements AutoCloseable {

    // The only gauges of these names that this class puts on the default registry. Later calls of register() only
    // count, and after the last instance has closed the same objects are registered again: unregister() has released
    // their names by then, so the registry accepts them, and it rejects an object that it holds already before it
    // adds it. PrometheusRegistry.register() adds a new collector before it checks the name and removes it again when
    // the name is taken, so a scrape that runs meanwhile gets the name twice. That can still happen in register()
    // when code outside this class has registered one of the names.
    private static final Map<String, GaugeWithCallback> GAUGES = newGauges();

    // The first instance to open registers the gauges, and the last one to close unregisters the ones it registered.
    // Guarded by the class lock.
    private static int openInstances;
    private static final List<GaugeWithCallback> REGISTERED_GAUGES = new ArrayList<>(GAUGES.size());

    // Guarded by the class lock
    private boolean closed;

    private NettyDirectMemoryGauges() {
    }

    /**
     * Registers the gauges on {@link PrometheusRegistry#defaultRegistry}, unless an instance that an earlier call
     * returned is still open.
     *
     * @return an instance to close once the caller no longer exports the gauges; closing the last open instance
     *     unregisters them
     */
    public static synchronized NettyDirectMemoryGauges register() {
        if (openInstances++ == 0) {
            GAUGES.forEach((name, gauge) -> {
                try {
                    PrometheusRegistry.defaultRegistry.register(gauge);
                    REGISTERED_GAUGES.add(gauge);
                } catch (IllegalArgumentException nameIsTaken) {
                    // Code that does not use this class has registered the name
                    log.debug().attr("gauge", name).exceptionMessage(nameIsTaken)
                            .log("Prometheus gauge is already registered");
                }
            });
        }
        return new NettyDirectMemoryGauges();
    }

    /**
     * Registers new gauges on a registry other than {@link PrometheusRegistry#defaultRegistry}, for as long as that
     * registry exists. Use {@link #register()} for the default registry.
     *
     * @throws IllegalArgumentException if one of the names is registered on {@code registry} already
     */
    public static void registerOn(PrometheusRegistry registry) {
        newGauges().values().forEach(registry::register);
    }

    private static Map<String, GaugeWithCallback> newGauges() {
        return Map.of(
                "jvm_memory_direct_bytes_used", gauge("jvm_memory_direct_bytes_used",
                        JvmMetrics::getJvmDirectMemoryUsed),
                "jvm_memory_direct_bytes_max", gauge("jvm_memory_direct_bytes_max",
                        DirectMemoryUtils::jvmMaxDirectMemory));
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

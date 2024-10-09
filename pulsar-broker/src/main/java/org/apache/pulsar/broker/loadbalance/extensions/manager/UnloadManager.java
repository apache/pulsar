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
package org.apache.pulsar.broker.loadbalance.extensions.manager;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigning;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.Histogram;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;

/**
 * Unload manager.
 */
@Slf4j
public class UnloadManager implements StateChangeListener {

    private final UnloadCounter counter;
    private final Map<String, CompletableFuture<Void>> inFlightUnloadRequest;
    private final String brokerId;

    @VisibleForTesting
    public enum LatencyMetric {
        UNLOAD(buildHistogram(
            "brk_lb_unload_latency", "Total time duration of unload operations on source brokers"), true, false),
        ASSIGN(buildHistogram(
            "brk_lb_assign_latency", "Time spent in the load balancing ASSIGN state on destination brokers"),
                false, true),
        RELEASE(buildHistogram(
            "brk_lb_release_latency", "Time spent in the load balancing RELEASE state on source brokers"), true, false),
        DISCONNECT(buildHistogram(
            "brk_lb_disconnect_latency", "Time spent in the load balancing disconnected state on source brokers"),
                true, false);

        private static Histogram buildHistogram(String name, String help) {
            return Histogram.build(name, help).unit("ms").labelNames("broker", "metric").
                    buckets(new double[] {1.0, 10.0, 100.0, 200.0, 1000.0}).register();
        }
        private static final long OP_TIMEOUT_NS = TimeUnit.HOURS.toNanos(1);

        private final Histogram histogram;
        private final Map<String, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();
        private final boolean isSourceBrokerMetric;
        private final boolean isDestinationBrokerMetric;

        LatencyMetric(Histogram histogram, boolean isSourceBrokerMetric, boolean isDestinationBrokerMetric) {
            this.histogram = histogram;
            this.isSourceBrokerMetric = isSourceBrokerMetric;
            this.isDestinationBrokerMetric = isDestinationBrokerMetric;
        }

        public void beginMeasurement(String serviceUnit, String brokerId, ServiceUnitStateData data) {
            if ((isSourceBrokerMetric && brokerId.equals(data.sourceBroker()))
                    || (isDestinationBrokerMetric && brokerId.equals(data.dstBroker()))) {
                var startTimeNs = System.nanoTime();
                futures.computeIfAbsent(serviceUnit, ignore -> {
                    var future = new CompletableFuture<Void>();
                    future.completeOnTimeout(null, OP_TIMEOUT_NS, TimeUnit.NANOSECONDS).
                            thenAccept(__ -> {
                                var durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs);
                                log.info("Operation {} for service unit {} took {} ms", this, serviceUnit, durationMs);
                                histogram.labels(brokerId, "bundleUnloading").observe(durationMs);
                            }).whenComplete((__, throwable) -> futures.remove(serviceUnit, future));
                    return future;
                });
            }
        }

        public void endMeasurement(String serviceUnit) {
            var future = futures.get(serviceUnit);
            if (future != null) {
                future.complete(null);
            }
        }
    }

    public UnloadManager(UnloadCounter counter, String brokerId) {
        this.counter = counter;
        this.brokerId = Objects.requireNonNull(brokerId);
        inFlightUnloadRequest = new ConcurrentHashMap<>();
    }

    private void complete(String serviceUnit, Throwable ex) {
        LatencyMetric.UNLOAD.endMeasurement(serviceUnit);
        LatencyMetric.DISCONNECT.endMeasurement(serviceUnit);
        if (ex != null) {
            LatencyMetric.RELEASE.endMeasurement(serviceUnit);
            LatencyMetric.ASSIGN.endMeasurement(serviceUnit);
        }

        inFlightUnloadRequest.computeIfPresent(serviceUnit, (__, future) -> {
            if (!future.isDone()) {
                if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(null);
                }
            }
            return null;
        });
    }

    public CompletableFuture<Void> waitAsync(CompletableFuture<Void> eventPubFuture,
                                             String bundle,
                                             UnloadDecision decision,
                                             long timeout,
                                             TimeUnit timeoutUnit) {
        return eventPubFuture.thenCompose(__ -> inFlightUnloadRequest.computeIfAbsent(bundle, ignore -> {
            if (log.isDebugEnabled()) {
                log.debug("Handle unload bundle: {}, timeout: {} {}", bundle, timeout, timeoutUnit);
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.orTimeout(timeout, timeoutUnit).whenComplete((v, ex) -> {
                if (ex != null) {
                    inFlightUnloadRequest.remove(bundle);
                    log.warn("Failed to wait unload for serviceUnit: {}", bundle, ex);
                }
            });
            return future;
        })).whenComplete((__, ex) -> {
            if (ex != null) {
                counter.update(Failure, Unknown);
                log.warn("Failed to unload bundle: {}", bundle, ex);
                return;
            }
            log.info("Complete unload bundle: {}", bundle);
            counter.update(decision);
        });
    }

    @Override
    public void beforeEvent(String serviceUnit, ServiceUnitStateData data) {
        if (log.isDebugEnabled()) {
            log.debug("Handling arrival of {} for service unit {}", data, serviceUnit);
        }
        ServiceUnitState state = ServiceUnitStateData.state(data);
        switch (state) {
            case Free, Owned -> LatencyMetric.DISCONNECT.beginMeasurement(serviceUnit, brokerId, data);
            case Releasing -> {
                LatencyMetric.RELEASE.beginMeasurement(serviceUnit, brokerId, data);
                LatencyMetric.UNLOAD.beginMeasurement(serviceUnit, brokerId, data);
            }
            case Assigning -> LatencyMetric.ASSIGN.beginMeasurement(serviceUnit, brokerId, data);
        }
    }

    @Override
    public void handleEvent(String serviceUnit, ServiceUnitStateData data, Throwable t) {
        ServiceUnitState state = ServiceUnitStateData.state(data);

        if ((state == Owned || state == Assigning) && StringUtils.isBlank(data.sourceBroker())) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping {} for service unit {} from the assignment command.", data, serviceUnit);
            }
            return;
        }

        if (t != null) {
            if (log.isDebugEnabled()) {
                log.debug("Handling {} for service unit {} with exception.", data, serviceUnit, t);
            }
            complete(serviceUnit, t);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Handling {} for service unit {}", data, serviceUnit);
        }

        switch (state) {
            case Free -> {
                if (!data.force()) {
                    complete(serviceUnit, t);
                }
            }
            case Init -> {
                checkArgument(data == null, "Init state must be associated with null data");
                complete(serviceUnit, t);
            }
            case Owned -> complete(serviceUnit, t);
            case Releasing -> LatencyMetric.RELEASE.endMeasurement(serviceUnit);
            case Assigning -> LatencyMetric.ASSIGN.endMeasurement(serviceUnit);
        }
    }

    public void close() {
        inFlightUnloadRequest.forEach((bundle, future) -> {
            if (!future.isDone()) {
                String msg = String.format("Unloading bundle: %s, but the unload manager already closed.", bundle);
                log.warn(msg);
                future.completeExceptionally(new IllegalStateException(msg));
            }
        });
        inFlightUnloadRequest.clear();
    }
}

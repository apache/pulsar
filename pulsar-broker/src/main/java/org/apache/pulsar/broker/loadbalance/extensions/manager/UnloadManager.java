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

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision;
import org.apache.pulsar.broker.stats.prometheus.metrics.Summary;

/**
 * Unload manager.
 */
@Slf4j
public class UnloadManager implements StateChangeListener {

    private final UnloadCounter counter;
    private final Map<String, CompletableFuture<Void>> inFlightUnloadRequest;
    private final String lookupServiceAddress;
    private final LatencyMetric unloadLatency;
    private final LatencyMetric assignLatency;
    private final LatencyMetric releaseLatency;


    private class LatencyMetric {

        private static final long OP_TIMEOUT_NS = TimeUnit.HOURS.toNanos(1);
        private static final double QUANTILES[] = {0.0, 0.50, 0.95, 0.99, 0.999, 0.9999, 1.0};
        private static final String LABEL_NAMES[] = {"broker"};

        private final Summary.Child summary;
        private final Map<String, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();
        private final String operation;

        LatencyMetric(String name, String help, String operation) {
            var builder = Summary.build(name, help).labelNames(LABEL_NAMES);
            for (var quantile: QUANTILES) {
                builder.quantile(quantile);
            }
            this.summary = builder.register().labels(lookupServiceAddress);
            this.operation = operation;
        }

        public void beginMeasurement(String serviceUnit) {
            var startTimeNs = System.nanoTime();
            futures.computeIfAbsent(serviceUnit, ignore -> {
                var future = new CompletableFuture<Void>();
                future.completeOnTimeout(null, OP_TIMEOUT_NS, TimeUnit.NANOSECONDS).
                        thenAccept(__ -> {
                            var durationNs = System.nanoTime() - startTimeNs;
                            log.info("Operation {} for service unit {} took {} ns", operation, serviceUnit, durationNs);
                            summary.observe(durationNs, TimeUnit.NANOSECONDS);
                        }).whenComplete((__, throwable) -> futures.remove(serviceUnit, future));
                return future;
            });
        }

        public void endMeasurement(String serviceUnit) {
            var future = futures.get(serviceUnit);
            if (future != null) {
                future.complete(null);
            }
        }
    }

    public UnloadManager(PulsarService pulsar, UnloadCounter counter) {
        this.counter = counter;
        inFlightUnloadRequest = new ConcurrentHashMap<>();
        lookupServiceAddress = Objects.requireNonNull(pulsar.getLookupServiceAddress());
        unloadLatency =
            new LatencyMetric("brk_lb_unload_latency", "Total time duration of unload operations", "UNLOAD");
        assignLatency =
            new LatencyMetric("brk_lb_assign_latency", "Time spent in the load balancing ASSIGN state", "ASSIGN");
        releaseLatency =
            new LatencyMetric("brk_lb_release_latency", "Time spent in the load balancing RELEASE state", "RELEASE");
    }

    private void complete(String serviceUnit, Throwable ex) {
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

        unloadLatency.endMeasurement(serviceUnit);
        assignLatency.endMeasurement(serviceUnit);
        if (ex != null) {
            releaseLatency.endMeasurement(serviceUnit);
        }
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
    public void handleEvent(String serviceUnit, ServiceUnitStateData data, Throwable t) {
        if (t != null && inFlightUnloadRequest.containsKey(serviceUnit)) {
            if (log.isDebugEnabled()) {
                log.debug("Handling {} for service unit {} with exception.", data, serviceUnit, t);
            }
            complete(serviceUnit, t);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Handling {} for service unit {}", data, serviceUnit);
        }
        ServiceUnitState state = ServiceUnitStateData.state(data);
        switch (state) {
            case Free, Owned -> complete(serviceUnit, t);
            case Releasing -> recordReleaseLatency(serviceUnit, data);
            case Assigning -> recordAssigningLatency(serviceUnit, data);
        }
    }

    private void recordReleaseLatency(String serviceUnit, ServiceUnitStateData data) {
        if (lookupServiceAddress.equals(data.sourceBroker())) {
            releaseLatency.beginMeasurement(serviceUnit);
            unloadLatency.beginMeasurement(serviceUnit);
        } else if (lookupServiceAddress.equals(data.dstBroker())) {
            unloadLatency.beginMeasurement(serviceUnit);
        }
    }

   private void recordAssigningLatency(String serviceUnit, ServiceUnitStateData data) {
        if (lookupServiceAddress.equals(data.sourceBroker())) {
            releaseLatency.endMeasurement(serviceUnit);
        } else if (lookupServiceAddress.equals(data.dstBroker())) {
            assignLatency.beginMeasurement(serviceUnit);
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

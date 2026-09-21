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

import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Unknown;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.CustomLog;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;

/**
 * Split manager.
 */
@CustomLog
public class SplitManager implements StateChangeListener {


    private final Map<String, CompletableFuture<Void>> inFlightSplitRequests;

    private final SplitCounter counter;

    public SplitManager(SplitCounter splitCounter) {
        this.inFlightSplitRequests = new ConcurrentHashMap<>();
        this.counter = splitCounter;
    }

    private void complete(String serviceUnit, Throwable ex) {
        CompletableFuture<Void> future = inFlightSplitRequests.remove(serviceUnit);
        if (future == null || future.isDone()) {
            return;
        }
        if (ex != null) {
            future.completeExceptionally(ex);
        } else {
            future.complete(null);
        }
    }

    public CompletableFuture<Void> waitAsync(CompletableFuture<Void> eventPubFuture,
                                             String bundle,
                                             SplitDecision decision,
                                             long timeout,
                                             TimeUnit timeoutUnit) {
        return eventPubFuture.thenCompose(__ -> {
            CompletableFuture<Void> future = inFlightSplitRequests.computeIfAbsent(bundle, ignore -> {
                log.info().attr("bundle", bundle).attr("timeout", timeout)
                        .attr("timeoutUnit", timeoutUnit)
                        .log("Published the bundle split event for bundle: . "
                                + "Waiting the split event to complete. Timeout");
                return new CompletableFuture<Void>().orTimeout(timeout, timeoutUnit);
            });
            // Return the dependent stage so callers cannot observe completion before timeout cleanup finishes.
            return future.whenComplete((v, ex) -> {
                if (ex instanceof TimeoutException && inFlightSplitRequests.remove(bundle, future)) {
                    log.warn().attr("bundle", bundle).exception(ex)
                            .log("Timed out while waiting for the bundle split event");
                }
            });
        })
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error().attr("bundle", bundle).exception(ex)
                                .log("Failed the bundle split event for bundle");
                        counter.update(Failure, Unknown);
                    } else {
                        log.info().attr("bundle", bundle).log("Completed the bundle split event for bundle");
                        counter.update(decision);
                    }
                });
    }

    @Override
    public void handleEvent(String serviceUnit, ServiceUnitStateData data, Throwable t) {
        ServiceUnitState state = ServiceUnitStateData.state(data);
        if (t != null && inFlightSplitRequests.containsKey(serviceUnit)) {
            this.complete(serviceUnit, t);
            return;
        }
        switch (state) {
            case Init -> this.complete(serviceUnit, t);
            default -> {
                log.debug().attr("handling", data).attr("unit", serviceUnit).log("Handling for service unit");
            }
        }
    }

    public void close() {
        inFlightSplitRequests.forEach((bundle, future) -> {
            if (inFlightSplitRequests.remove(bundle, future) && !future.isDone()) {
                String msg = String.format("Splitting bundle: %s, but the manager already closed.", bundle);
                log.warn(msg);
                future.completeExceptionally(new IllegalStateException(msg));
            }
        });
    }

    @VisibleForTesting
    int getInFlightSplitRequestCount() {
        return inFlightSplitRequests.size();
    }
}

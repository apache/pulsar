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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;

/**
 * Split manager.
 */
@Slf4j
public class SplitManager implements StateChangeListener {

    record InFlightSplitRequest(SplitDecision splitDecision, CompletableFuture<Void> future) {
    }

    private final Map<String, InFlightSplitRequest> inFlightSplitRequests;

    private final SplitCounter counter;

    public SplitManager(SplitCounter splitCounter) {
        this.inFlightSplitRequests = new ConcurrentHashMap<>();
        this.counter = splitCounter;
    }

    private void complete(String serviceUnit, Throwable ex) {
        inFlightSplitRequests.computeIfPresent(serviceUnit, (__, inFlightSplitRequest) -> {
            var future = inFlightSplitRequest.future;
            if (!future.isDone()) {
                if (ex != null) {
                    counter.update(Failure, Unknown);
                    future.completeExceptionally(ex);
                    log.error("Failed the bundle split event: {}", serviceUnit, ex);
                } else {
                    counter.update(inFlightSplitRequest.splitDecision);
                    future.complete(null);
                    log.info("Completed the bundle split event: {}", serviceUnit);
                }
            }
            return null;
        });
    }

    public CompletableFuture<Void> waitAsync(CompletableFuture<Void> eventPubFuture,
                                             String bundle,
                                             SplitDecision decision,
                                             long timeout,
                                             TimeUnit timeoutUnit) {
        return eventPubFuture
                .thenCompose(__ -> inFlightSplitRequests.computeIfAbsent(bundle, ignore -> {
                    log.info("Published the bundle split event for bundle:{}. "
                                    + "Waiting the split event to complete. Timeout: {} {}",
                            bundle, timeout, timeoutUnit);
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    future.orTimeout(timeout, timeoutUnit).whenComplete((v, ex) -> {
                        if (ex != null) {
                            inFlightSplitRequests.remove(bundle);
                            log.warn("Timed out while waiting for the bundle split event: {}", bundle, ex);
                        }
                    });
                    return new InFlightSplitRequest(decision, future);
                }).future)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish the bundle split event for bundle:{}. Skipping wait.", bundle);
                        counter.update(Failure, Unknown);
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
            case Deleted, Owned, Init -> this.complete(serviceUnit, t);
            default -> {
                if (log.isDebugEnabled()) {
                    log.debug("Handling {} for service unit {}", data, serviceUnit);
                }
            }
        }
    }

    public void close() {
        inFlightSplitRequests.forEach((bundle, inFlightSplitRequest) -> {
            if (!inFlightSplitRequest.future.isDone()) {
                String msg = String.format("Splitting bundle: %s, but the manager already closed.", bundle);
                log.warn(msg);
                inFlightSplitRequest.future.completeExceptionally(new IllegalStateException(msg));
            }
        });
        inFlightSplitRequests.clear();
    }
}

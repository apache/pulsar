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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
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

    public UnloadManager(UnloadCounter counter) {
        this.counter = counter;
        this.inFlightUnloadRequest = new ConcurrentHashMap<>();
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
        ServiceUnitState state = ServiceUnitStateData.state(data);
        switch (state) {
            case Free, Owned -> this.complete(serviceUnit, t);
            default -> {
                if (log.isDebugEnabled()) {
                    log.debug("Handling {} for service unit {}", data, serviceUnit);
                }
            }
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

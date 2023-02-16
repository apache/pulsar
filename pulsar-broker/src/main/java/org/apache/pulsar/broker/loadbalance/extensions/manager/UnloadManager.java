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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;

@Slf4j
public class UnloadManager {

    private final PulsarService pulsar;

    private final Map<String, CompletableFuture<Void>> inFlightUnloadRequest;

    public UnloadManager(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.inFlightUnloadRequest = new ConcurrentHashMap<>();
    }

    public void completeUnload(String serviceUnit) {
        inFlightUnloadRequest.computeIfPresent(serviceUnit, (__, future) -> {
            if (!future.isDone()) {
                future.complete(null);
                if (log.isDebugEnabled()) {
                    log.debug("Complete unload bundle: {}", serviceUnit);
                }
            }
            return null;
        });
    }

    public CompletableFuture<Void> handleUnload(String bundle, long timeout, TimeUnit timeoutUnit) {
        return inFlightUnloadRequest.computeIfAbsent(bundle, __ -> {
            if (log.isDebugEnabled()) {
                log.debug("Handle unload bundle: {}, timeout: {} {}", bundle, timeout, timeoutUnit);
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            ScheduledFuture<?> taskTimeout = pulsar.getExecutor().schedule(() -> {
                if (!future.isDone()) {
                    String msg = String.format("Unloading bundle: %s has timed out, cancel the future.", bundle);
                    log.warn(msg);
                    // Complete the future with error
                    future.completeExceptionally(new TimeoutException(msg));
                }
            }, timeout, timeoutUnit);

            future.whenComplete((r, ex) -> taskTimeout.cancel(true));
            return future;
        });
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

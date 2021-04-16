/**
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
package org.apache.pulsar.broker.service;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Waits for termination of {@link ExecutorService}s that have been shutdown.
 *
 * The executors will be terminated forcefully after the timeout or when the future is cancelled.
 *
 * Designed to be used via the API in {@link GracefulExecutorServicesShutdown}
 */
@Slf4j
class GracefulExecutorServicesTerminationHandler {
    private final ScheduledExecutorService shutdownScheduler = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory(getClass().getSimpleName()));
    private final List<ExecutorService> executors;
    private final CompletableFuture<Void> future;
    private final long timeoutMs;
    private final long terminatedStatusPollingInterval;

    GracefulExecutorServicesTerminationHandler(long timeoutMs, List<ExecutorService> executorServices) {
        this.timeoutMs = Math.max(timeoutMs, 1L);
        this.terminatedStatusPollingInterval = Math.min(Math.max(timeoutMs / 100, 10), timeoutMs);
        executors = executorServices.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        future = new CompletableFuture<>();
    }

    CompletableFuture<Void> startTerminationHandler() {
        log.info("Starting shutdown handler for {} executors.", executors.size());
        for (ExecutorService executor : executors) {
            if (!executor.isShutdown()) {
                throw new IllegalStateException(
                        String.format("Executor %s should have been shutdown before entering the shutdown handler.",
                                executor));
            }
        }
        FutureUtil.whenCancelledOrTimedOut(future, () -> {
            terminateExecutorsAndShutdown();
        });
        checkIfExecutorsHaveBeenTerminated();
        if (!shutdownScheduler.isShutdown()) {
            try {
                shutdownScheduler.schedule(this::terminateExecutorsAndShutdown, timeoutMs, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // ignore
            }
        }
        return future;
    }

    private void terminateExecutorsAndShutdown() {
        for (ExecutorService executor : executors) {
            if (!executor.isTerminated()) {
                log.info("Shutting down forcefully executor {}", executor);
                for (Runnable runnable : executor.shutdownNow()) {
                    log.info("Execution in progress for runnable instance of {}: {}", runnable.getClass(),
                            runnable);
                }
            }
        }
        shutdown();
    }

    private void shutdown() {
        if (!shutdownScheduler.isShutdown()) {
            log.info("Shutting down scheduler.");
            shutdownScheduler.shutdown();
        }
    }

    private void scheduleCheck() {
        if (!shutdownScheduler.isShutdown()) {
            try {
                shutdownScheduler.schedule(this::checkIfExecutorsHaveBeenTerminated, terminatedStatusPollingInterval,
                        TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // ignore
            }
        }
    }

    private void checkIfExecutorsHaveBeenTerminated() {
        if (executors.stream().filter(executor -> !executor.isTerminated()).count() > 0) {
            scheduleCheck();
        } else {
            log.info("Shutdown completed.");
            future.complete(null);
            shutdown();
        }
    }
}

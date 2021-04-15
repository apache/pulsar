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
 * Shuts down one or many {@link ExecutorService}s in a graceful way.
 *
 * The executors will be terminated forcefully after the timeout or when the future is cancelled.
 *
 * Designed to be used via the API in {@link GracefulExecutorServicesShutdown}
 */
@Slf4j
class GracefulExecutorServicesShutdownHandler {
    private final ScheduledExecutorService shutdownScheduler = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory(getClass().getSimpleName()));
    private final List<ExecutorService> executors;
    private final CompletableFuture<Void> future;
    private final long timeoutMs;
    private final long shutdownStatusPollingInterval;

    GracefulExecutorServicesShutdownHandler(long timeoutMs, List<ExecutorService> executorServices) {
        this.timeoutMs = Math.max(timeoutMs, 1L);
        this.shutdownStatusPollingInterval = Math.min(Math.max(timeoutMs / 100, 10), timeoutMs);
        executors = executorServices.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        future = new CompletableFuture<>();
    }

    CompletableFuture<Void> startShutdownHandler() {
        log.info("Shutting down {} executors.", executors.size());
        executors.forEach(ExecutorService::shutdown);
        FutureUtil.whenCancelledOrTimedOut(future, () -> {
            terminate();
        });
        checkIfExecutorsHaveBeenShutdown();
        if (!shutdownScheduler.isShutdown()) {
            try {
                shutdownScheduler.schedule(this::terminate, timeoutMs, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // ignore
            }
        }
        return future;
    }

    private void terminate() {
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
                shutdownScheduler.schedule(this::checkIfExecutorsHaveBeenShutdown, shutdownStatusPollingInterval,
                        TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // ignore
            }
        }
    }

    private void checkIfExecutorsHaveBeenShutdown() {
        if (executors.stream().filter(executor -> !executor.isTerminated()).count() > 0) {
            scheduleCheck();
        } else {
            log.info("Shutdown completed.");
            future.complete(null);
            shutdown();
        }
    }
}

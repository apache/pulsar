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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
    private static final long SHUTDOWN_THREAD_COMPLETION_TIMEOUT_NANOS = Duration.ofMillis(100L).toNanos();
    private final List<ExecutorService> executors;
    private final CompletableFuture<Void> future;
    private final Duration shutdownTimeout;
    private final Duration terminationTimeout;
    private final CountDownLatch shutdownThreadCompletedLatch = new CountDownLatch(1);

    GracefulExecutorServicesTerminationHandler(Duration shutdownTimeout, Duration terminationTimeout,
                                               List<ExecutorService> executorServices) {
        this.shutdownTimeout = shutdownTimeout;
        this.terminationTimeout = terminationTimeout;
        this.executors = Collections.unmodifiableList(new ArrayList<>(executorServices));
        this.future = new CompletableFuture<>();
        log.info("Starting termination handler for {} executors.", executors.size());
        for (ExecutorService executor : executors) {
            if (!executor.isShutdown()) {
                throw new IllegalStateException(
                        String.format("Executor %s should have been shutdown before entering the termination handler.",
                                executor));
            }
        }
        if (haveExecutorsBeenTerminated()) {
            markShutdownCompleted();
        } else {
            if (shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
                terminateExecutors();
                markShutdownCompleted();
            } else {
                Thread shutdownWaitingThread = new Thread(this::awaitShutdown, getClass().getSimpleName());
                shutdownWaitingThread.setDaemon(false);
                shutdownWaitingThread.setUncaughtExceptionHandler((thread, exception) -> {
                  log.error("Uncaught exception in shutdown thread {}", thread, exception);
                });
                shutdownWaitingThread.start();
                FutureUtil.whenCancelledOrTimedOut(future, () -> {
                    shutdownWaitingThread.interrupt();
                    waitUntilShutdownWaitingThreadIsCompleted();
                });
            }
        }
    }

    public CompletableFuture<Void> getFuture() {
        return future;
    }

    private boolean haveExecutorsBeenTerminated() {
        return executors.stream().allMatch(ExecutorService::isTerminated);
    }

    private void markShutdownCompleted() {
        log.info("Shutdown completed.");
        future.complete(null);
    }

    private void awaitShutdown() {
        try {
            awaitTermination(shutdownTimeout);
            terminateExecutors();
            markShutdownCompleted();
        } catch (Exception e) {
            log.error("Error in termination handler", e);
            future.completeExceptionally(e);
        } finally {
            shutdownThreadCompletedLatch.countDown();
        }
    }

    private boolean awaitTermination(Duration timeout) {
        if (!timeout.isZero() && !timeout.isNegative()) {
            long awaitUntilNanos = System.nanoTime() + timeout.toNanos();
            while (!Thread.currentThread().isInterrupted() && System.nanoTime() < awaitUntilNanos) {
                int activeExecutorsCount = executors.size();
                for (ExecutorService executor : executors) {
                    long remainingTimeNanos = awaitUntilNanos - System.nanoTime();
                    if (remainingTimeNanos > 0) {
                        try {
                            if (executor.isTerminated()
                                    || executor.awaitTermination(remainingTimeNanos, TimeUnit.NANOSECONDS)) {
                                activeExecutorsCount--;
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                if (activeExecutorsCount == 0) {
                    return true;
                }
            }
        }
        return haveExecutorsBeenTerminated();
    }

    private void terminateExecutors() {
        for (ExecutorService executor : executors) {
            if (!executor.isTerminated()) {
                log.info("Shutting down forcefully executor {}", executor);
                executor.shutdownNow();
            }
        }
        if (!Thread.currentThread().isInterrupted() && !awaitTermination(terminationTimeout)) {
            for (ExecutorService executor : executors) {
                if (!executor.isTerminated()) {
                    log.warn("Executor {} didn't shutdown after waiting for termination.", executor);
                    for (Runnable runnable : executor.shutdownNow()) {
                        log.info("Execution in progress for runnable instance of {}: {}", runnable.getClass(),
                                runnable);
                    }
                }
            }
        }
    }

    private void waitUntilShutdownWaitingThreadIsCompleted() {
        try {
            shutdownThreadCompletedLatch.await(terminationTimeout.toNanos()
                    + SHUTDOWN_THREAD_COMPLETION_TIMEOUT_NANOS, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

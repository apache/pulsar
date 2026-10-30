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
package org.apache.bookkeeper.replication;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Parallelizes independent test teardown calls without changing elector lifecycle behavior. */
final class ParallelAuditorShutdown {
    private static final int MAX_CONCURRENT_SHUTDOWNS = 6;
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(90);

    private ParallelAuditorShutdown() {
    }

    static void shutdown(Collection<AuditorElector> electors) throws Exception {
        shutdown(electors, SHUTDOWN_TIMEOUT);
    }

    @VisibleForTesting
    static void shutdown(Collection<AuditorElector> electors, Duration timeout) throws Exception {
        shutdownAll(electors, timeout);
    }

    @FunctionalInterface
    interface Cleanup {
        void run() throws Exception;
    }

    static void cleanup(Cleanup... cleanups) throws Exception {
        cleanup(Arrays.asList(cleanups));
    }

    static void cleanup(Collection<Cleanup> cleanups) throws Exception {
        Outcome outcome = new Outcome();
        if (Thread.interrupted()) {
            outcome.interrupted = true;
            outcome.addFailure(new InterruptedException("Interrupted before auditor test cleanup"));
        }
        try {
            for (Cleanup cleanup : cleanups) {
                try {
                    cleanup.run();
                } catch (Throwable error) {
                    if (error instanceof InterruptedException) {
                        outcome.interrupted = true;
                    }
                    outcome.addFailure(error);
                } finally {
                    // Let later blocking cleanup run even when a previous step restored this flag.
                    if (Thread.interrupted()) {
                        outcome.interrupted = true;
                    }
                }
            }
        } finally {
            if (outcome.interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        outcome.rethrowFailure();
    }

    private static void shutdownAll(Collection<AuditorElector> electors, Duration timeout) throws Exception {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Shutdown timeout must be positive");
        }
        Set<AuditorElector> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        List<AuditorElector> distinct = new ArrayList<>();
        for (AuditorElector elector : electors) {
            if (seen.add(Objects.requireNonNull(elector, "elector"))) {
                distinct.add(elector);
            }
        }
        if (distinct.isEmpty()) {
            return;
        }

        List<Thread> helperThreads = Collections.synchronizedList(new ArrayList<>());
        ConcurrentLinkedQueue<Throwable> uncaughtFailures = new ConcurrentLinkedQueue<>();
        ThreadFactory defaultFactory = new DefaultThreadFactory("parallel-auditor-shutdown");
        ExecutorService executor = Executors.newFixedThreadPool(
                Math.min(MAX_CONCURRENT_SHUTDOWNS, distinct.size()), task -> {
                    Thread thread = defaultFactory.newThread(task);
                    Thread.UncaughtExceptionHandler originalHandler = thread.getUncaughtExceptionHandler();
                    thread.setUncaughtExceptionHandler((failedThread, error) -> {
                        uncaughtFailures.add(error);
                        if (originalHandler != null) {
                            try {
                                originalHandler.uncaughtException(failedThread, error);
                            } catch (Throwable handlerError) {
                                uncaughtFailures.add(handlerError);
                            }
                        }
                    });
                    helperThreads.add(thread);
                    return thread;
                });
        List<Future<?>> shutdowns = new ArrayList<>();
        Outcome outcome = new Outcome();
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            for (AuditorElector elector : distinct) {
                try {
                    shutdowns.add(executor.submit(() -> {
                        elector.shutdown();
                        return null;
                    }));
                } catch (Throwable error) {
                    outcome.addFailure(error);
                }
            }
        } finally {
            executor.shutdown();
        }

        try {
            boolean deadlineExceeded = false;
            while (!executor.isTerminated()) {
                long remaining = deadline - System.nanoTime();
                if (!deadlineExceeded && remaining <= 0) {
                    deadlineExceeded = true;
                    outcome.addFailure(new TimeoutException("Auditor shutdown exceeded " + timeout));
                }
                try {
                    // A timeout is a test failure, not permission to tear down the cluster while
                    // close calls still run. Do not cancel or interrupt dependency cleanup.
                    executor.awaitTermination(deadlineExceeded ? TimeUnit.SECONDS.toNanos(1) : remaining,
                            TimeUnit.NANOSECONDS);
                } catch (InterruptedException error) {
                    outcome.interrupted = true;
                    outcome.addFailure(error);
                }
            }
            for (Future<?> shutdown : shutdowns) {
                boolean observed = false;
                while (!observed) {
                    try {
                        // Executor termination means every submitted shutdown has finished.
                        shutdown.get();
                        observed = true;
                    } catch (InterruptedException error) {
                        outcome.interrupted = true;
                        outcome.addFailure(error);
                    } catch (ExecutionException error) {
                        outcome.addFailure(error.getCause());
                        observed = true;
                    } catch (CancellationException error) {
                        outcome.addFailure(error);
                        observed = true;
                    }
                }
            }
            List<Thread> threadsToJoin;
            synchronized (helperThreads) {
                threadsToJoin = new ArrayList<>(helperThreads);
            }
            for (Thread thread : threadsToJoin) {
                // Executor termination precedes FastThreadLocalThread's outer cleanup.
                // Join the actual threads before releasing the remaining fixture resources.
                while (thread.isAlive()) {
                    long remaining = deadline - System.nanoTime();
                    if (!deadlineExceeded && remaining <= 0) {
                        deadlineExceeded = true;
                        outcome.addFailure(new TimeoutException("Auditor shutdown exceeded " + timeout));
                    }
                    try {
                        TimeUnit.NANOSECONDS.timedJoin(thread,
                                deadlineExceeded ? TimeUnit.SECONDS.toNanos(1) : remaining);
                    } catch (InterruptedException error) {
                        outcome.interrupted = true;
                        outcome.addFailure(error);
                    }
                }
            }
            for (Throwable error : uncaughtFailures) {
                outcome.addFailure(error);
            }
            if (!deadlineExceeded && System.nanoTime() - deadline >= 0) {
                outcome.addFailure(new TimeoutException("Auditor shutdown exceeded " + timeout));
            }
        } finally {
            if (outcome.interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        outcome.rethrowFailure();
    }

    private static final class Outcome {
        private Throwable failure;
        private boolean interrupted;

        void addFailure(Throwable error) {
            if (failure == null) {
                failure = error;
            } else if (failure != error) {
                failure.addSuppressed(error);
            }
        }

        void rethrowFailure() throws Exception {
            if (failure instanceof Exception exception) {
                throw exception;
            } else if (failure instanceof Error error) {
                throw error;
            } else if (failure != null) {
                throw new Exception("Auditor test cleanup failed", failure);
            }
        }
    }
}

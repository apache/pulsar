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
package org.apache.pulsar.client.impl;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;

/**
 * Runs the periodic flushes of the consumers' acknowledgments grouping trackers with one scheduled task for each
 * executor and group time, instead of a task for each consumer.
 *
 * <p>A tracker flushes its grouped acks every {@code acknowledgementsGroupTime}. With a task of its own, a process with
 * thousands of consumers keeps thousands of periodic tasks in its event loops' scheduled task queues, and each run is
 * a removal from the queue and an insertion back into it. Here the flushes registered for an executor and a group time
 * share one fixed-delay task. A tracker requests its flush with {@link Registration#requestFlush()} when it has acks
 * to send, and each run of the task runs the flushes requested before it, one after the other, so that the trackers
 * without acks to send cost nothing. A flush runs at most one group time after its request, besides the executor's
 * delays and the other flushes of the run.
 *
 * <p>The shared tasks are kept in a map for the life of their registrations: the last registration to close cancels
 * its task and removes it, and an executor's termination removes its tasks, so that a map entry doesn't outlive the
 * consumers that use its executor, nor the executor when a client shuts down without closing its consumers.
 */
@CustomLog
final class AcknowledgmentsGroupingFlushes {
    private record Key(EventExecutor executor, long groupTimeMicros) {
    }

    private static final ConcurrentHashMap<Key, SharedFlush> SHARED_FLUSHES = new ConcurrentHashMap<>();

    private AcknowledgmentsGroupingFlushes() {
    }

    /** A registered flush, which {@link #close()} stops. */
    static final class Registration implements AutoCloseable {
        private final Key key;
        private final Runnable flush;
        private final AtomicBoolean closed = new AtomicBoolean();
        // whether the flush is in the shared flush's queue; cleared by the run that takes it, before it runs it
        private final AtomicBoolean queued = new AtomicBoolean();
        // set in the map's compute, before the registration is returned; cleared by close(), so that a closed
        // registration doesn't keep the shared flush and the other registrations
        private volatile SharedFlush shared;

        private Registration(Key key, Runnable flush) {
            this.key = key;
            this.flush = flush;
        }

        /**
         * Requests the flush, which the next run of the shared task runs. A flush requested while it is queued runs
         * once; one requested while it runs, or by itself, runs again in a later run.
         */
        void requestFlush() {
            SharedFlush current = shared;
            if (current != null && !queued.get() && queued.compareAndSet(false, true)) {
                current.requested.add(this);
            }
        }

        /** Stops the periodic flush; a flush that is running may complete. Closing again does nothing. */
        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            SharedFlush current = shared;
            shared = null;
            if (current == null) {
                return;
            }
            // a queued request isn't run; removed so that it doesn't wait for a run that may never come
            current.requested.remove(this);
            current.registrations.remove(this);
            SHARED_FLUSHES.computeIfPresent(key, (k, mapped) -> {
                if (mapped == current && current.registrations.isEmpty()) {
                    current.retire();
                    return null;
                }
                return mapped;
            });
        }
    }

    private static final class SharedFlush implements Runnable {
        private final Set<Registration> registrations = ConcurrentHashMap.newKeySet();
        private final Queue<Registration> requested = new ConcurrentLinkedQueue<>();
        // the flushes taken by a run, reused by the runs, which the executor runs one at a time
        private final List<Registration> running = new ArrayList<>();
        // set in the map's compute, before the map publishes the shared flush; null with a mocked executor in tests
        private ScheduledFuture<?> task;
        private Future<?> termination;
        private GenericFutureListener<Future<Object>> terminationListener;

        // Cancels the task, and stops listening to the executor's termination, which would keep this otherwise
        void retire() {
            if (task != null) {
                task.cancel(false);
            }
            if (termination != null) {
                termination.removeListener(terminationListener);
            }
        }

        // The executor terminated: no run takes the requests any more. Drops the references to the registrations,
        // which aren't flushed any more, so that a registration that is never closed doesn't keep the others
        void terminated() {
            registrations.clear();
            requested.clear();
        }

        @Override
        public void run() {
            // takes the requested flushes first, so that the flushes requested while they run wait for the next run
            Registration registration;
            while ((registration = requested.poll()) != null) {
                running.add(registration);
            }
            try {
                for (int i = 0; i < running.size(); i++) {
                    registration = running.get(i);
                    registration.queued.set(false);
                    // a registration closed after its request isn't flushed
                    if (registration.closed.get()) {
                        continue;
                    }
                    try {
                        registration.flush.run();
                    } catch (Throwable t) {
                        log.warn().exception(t).log("Flushing grouped acknowledgments failed");
                    }
                }
            } finally {
                running.clear();
            }
        }
    }

    /**
     * Registers {@code flush}, which runs on {@code executor} at most {@code groupTimeMicros} after each
     * {@link Registration#requestFlush() request}, with the other flushes of the same executor and group time, until
     * the returned registration is closed.
     *
     * @throws java.util.concurrent.RejectedExecutionException when the executor rejects the shared task, such as
     *                                                          after its shutdown
     */
    static Registration register(EventExecutor executor, long groupTimeMicros, Runnable flush) {
        Key key = new Key(executor, groupTimeMicros);
        Registration registration = new Registration(key, flush);
        SHARED_FLUSHES.compute(key, (k, shared) -> {
            if (shared == null) {
                shared = new SharedFlush();
                shared.task = executor.scheduleWithFixedDelay(shared, groupTimeMicros, groupTimeMicros,
                        TimeUnit.MICROSECONDS);
                SharedFlush created = shared;
                // the executor's termination drops its tasks; their registrations may never be closed. A mocked
                // executor in tests has neither a task nor a termination future
                created.termination = executor.terminationFuture();
                if (created.termination != null) {
                    created.terminationListener = future -> {
                        SHARED_FLUSHES.remove(k, created);
                        created.terminated();
                    };
                    created.termination.addListener(created.terminationListener);
                }
            }
            shared.registrations.add(registration);
            registration.shared = shared;
            return shared;
        });
        return registration;
    }

    /** The number of shared tasks of the executor, one for each group time with a registration. */
    @VisibleForTesting
    static long sharedTaskCount(EventExecutor executor) {
        return SHARED_FLUSHES.keySet().stream().filter(key -> key.executor() == executor).count();
    }
}

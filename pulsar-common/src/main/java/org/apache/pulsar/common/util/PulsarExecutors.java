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
package org.apache.pulsar.common.util;

import com.google.common.annotations.VisibleForTesting;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

/** Factory methods for Pulsar executors. */
public final class PulsarExecutors {
    private static final ExecutorQueueTrimmer QUEUE_TRIMMER = ExecutorQueueTrimmer.create();

    private PulsarExecutors() {
    }

    /**
     * Creates a single-thread executor using the default thread factory.
     *
     * @see #newSingleThreadExecutor(ThreadFactory)
     */
    public static ExecutorService newSingleThreadExecutor() {
        return newSingleThreadExecutor(Executors.defaultThreadFactory());
    }

    /**
     * Creates an executor with a lazily started worker and an unbounded growable array task queue.
     * Tasks execute sequentially, and the returned executor cannot be reconfigured to use more workers.
     * The queue reuses slots instead of allocating a linked node per task. When estimated backing-array
     * storage across these executors exceeds 5% of maximum heap, shared background maintenance trims
     * sparse queues toward a 4% target, retaining at least 64 slots and room for twice the current size.
     * Maintenance runs at most once every 30 seconds while over budget; live tasks are never discarded.
     * The budget counts array slots using estimated reference width; headers and task objects are excluded.
     * This is a retention budget, not a hard memory limit, and reclaiming old arrays depends on GC.
     *
     * <p>As with {@link Executors#newSingleThreadExecutor(ThreadFactory)}, garbage collection of the
     * returned wrapper triggers graceful shutdown as a fallback. This is not an idle timeout, and
     * cleanup timing is unspecified. Callers should explicitly shut down the executor when finished.
     *
     * @param threadFactory the factory used to create and replace the worker thread
     * @return a single-thread executor
     * @throws NullPointerException if the thread factory is null
     */
    public static ExecutorService newSingleThreadExecutor(ThreadFactory threadFactory) {
        return newSingleThreadExecutor(threadFactory, true);
    }

    /**
     * Creates a single-thread executor with optional garbage-collection cleanup.
     *
     * <p>When {@code autoShutdownOnGc} is false, the executor has no Cleaner registration or
     * GC-cleanup wrapper. This is suitable for components that manage executor shutdown explicitly.
     * The returned executor still prevents reconfiguration to multiple workers.
     *
     * @param threadFactory the factory used to create and replace the worker thread
     * @param autoShutdownOnGc whether an unreachable wrapper triggers graceful shutdown as a GC fallback
     * @return a single-thread executor
     * @throws NullPointerException if the thread factory is null
     * @see #newSingleThreadExecutor(ThreadFactory)
     */
    public static ExecutorService newSingleThreadExecutor(ThreadFactory threadFactory, boolean autoShutdownOnGc) {
        return newSingleThreadExecutor(threadFactory, autoShutdownOnGc, QUEUE_TRIMMER);
    }

    @VisibleForTesting
    static ExecutorService newSingleThreadExecutor(ThreadFactory threadFactory, boolean autoShutdownOnGc,
                                                   ExecutorQueueTrimmer group) {
        Objects.requireNonNull(threadFactory);
        GrowableArrayBlockingQueue<Runnable> queue = group.newQueue();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue, threadFactory) {
            @Override
            protected void terminated() {
                group.unregister(queue);
            }
        };
        return autoShutdownOnGc ? new AutoShutdownExecutorService(executor)
                : Executors.unconfigurableExecutorService(executor);
    }

    /**
     * Wraps an executor with a garbage-collection fallback for callers that do not explicitly shut it down.
     * After this wrapper becomes unreachable, a {@link Cleaner} requests graceful shutdown of the underlying
     * executor by calling {@link ThreadPoolExecutor#shutdown()}. Accepted tasks are allowed to finish without
     * interrupting the worker, and new submissions are rejected.
     *
     * <p>This preserves the automatic shutdown behavior of {@code Executors.newSingleThreadExecutor}.
     * This is not an idle timeout: GC-triggered cleanup requires an unreachable wrapper, and its timing
     * depends on garbage collection. Callers should explicitly shut down the executor when they no longer
     * need it rather than rely on this fallback.
     */
    @VisibleForTesting
    static final class AutoShutdownExecutorService extends AbstractExecutorService {
        private static final Cleaner CLEANER = Cleaner.create();

        private final ThreadPoolExecutor executor;
        private final Cleaner.Cleanable cleanable;

        AutoShutdownExecutorService(ThreadPoolExecutor executor) {
            this.executor = executor;
            // The cleanup action must not retain this facade.
            cleanable = CLEANER.register(this, executor::shutdown);
        }

        @Override
        public void execute(Runnable command) {
            try {
                executor.execute(command);
            } finally {
                // Keep the facade alive until admission completes, even when this is its last use.
                Reference.reachabilityFence(this);
            }
        }

        @Override
        public void shutdown() {
            try {
                executor.shutdown();
                cleanable.clean();
            } finally {
                Reference.reachabilityFence(this);
            }
        }

        @Override
        public List<Runnable> shutdownNow() {
            try {
                List<Runnable> pending = executor.shutdownNow();
                cleanable.clean();
                return pending;
            } finally {
                Reference.reachabilityFence(this);
            }
        }

        @Override
        public boolean isShutdown() {
            try {
                return executor.isShutdown();
            } finally {
                Reference.reachabilityFence(this);
            }
        }

        @Override
        public boolean isTerminated() {
            try {
                return executor.isTerminated();
            } finally {
                Reference.reachabilityFence(this);
            }
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            try {
                return executor.awaitTermination(timeout, unit);
            } finally {
                Reference.reachabilityFence(this);
            }
        }

        @VisibleForTesting
        void runCleanup() {
            cleanable.clean();
        }
    }
}

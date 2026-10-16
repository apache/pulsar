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
package org.apache.pulsar.broker.web;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.MathUtils;
import org.eclipse.jetty.util.ProcessorUtils;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.util.thread.ThreadPoolBudget;

/**
 * The thread pool of the Jetty web servers.
 *
 * <p>The pool keeps {@code numThreads} threads. Unless {@code maxThreads} is larger, it is a fixed size pool that
 * queues tasks when all of its threads are busy. When {@code maxThreads} is larger, the pool starts additional
 * threads, up to {@code maxThreads}, when a task is submitted and no thread is idle, and only queues tasks once
 * it has {@code maxThreads} threads. Threads beyond {@code numThreads} stop after being idle for the idle timeout,
 * see {@link #setIdleTimeout(int)}, which is {@link #DEFAULT_IDLE_TIMEOUT_MS} by default. A plain
 * {@link ThreadPoolExecutor} would start threads beyond its core size only once its queue is full.
 */
public class WebExecutorThreadPool extends ExecutorThreadPool {

    public static final int DEFAULT_IDLE_TIMEOUT_MS = 60_000;

    private static final ThreadLocal<WebExecutorThreadPool> CURRENT_POOL = new ThreadLocal<>();

    private final ThreadPoolExecutor executor;
    private final ThreadFactory threadFactory;
    private volatile boolean startAttempted;

    public WebExecutorThreadPool(int numThreads, String namePrefix) {
        this(numThreads, namePrefix, 8192);
    }

    public WebExecutorThreadPool(int numThreads, String namePrefix, int queueCapacity) {
        this(numThreads, 0, namePrefix, queueCapacity);
    }

    /**
     * @param numThreads the number of threads that the pool keeps
     * @param maxThreads the maximum number of threads; a value not larger than {@code numThreads}, such as 0,
     *                   means that the pool doesn't grow beyond {@code numThreads}
     * @param namePrefix the name prefix of the threads
     * @param queueCapacity the capacity of the queue for tasks that wait for a thread
     */
    public WebExecutorThreadPool(int numThreads, int maxThreads, String namePrefix, int queueCapacity) {
        this(createExecutor(numThreads, maxThreads, queueCapacity), numThreads, namePrefix);
    }

    private WebExecutorThreadPool(ThreadPoolExecutor executor, int numThreads, String namePrefix) {
        super(executor, -1, null);
        this.executor = executor;
        // the number of threads that are started when the pool starts
        setMinThreads(Math.min(8, numThreads));
        if (executor instanceof GrowingThreadPoolExecutor) {
            // Jetty sizes the reserved thread executor from the maximum number of threads, and its threads wait for
            // work without an idle timeout, so they wouldn't let the pool shrink. Size it from numThreads, like it
            // is sized for a pool that doesn't grow.
            setReservedThreads(Math.max(1,
                    MathUtils.ceilToNextPowerOfTwo(Math.min(ProcessorUtils.availableProcessors(), numThreads / 8))));
        }
        this.threadFactory = new DefaultThreadFactory(namePrefix);
    }

    private static ThreadPoolExecutor createExecutor(int numThreads, int maxThreads, int queueCapacity) {
        if (maxThreads <= numThreads) {
            // the same executor as ExecutorThreadPool(maxThreads, minThreads, queue) creates
            return new ThreadPoolExecutor(numThreads, numThreads, DEFAULT_IDLE_TIMEOUT_MS, TimeUnit.MILLISECONDS,
                    new BlockingArrayQueue<>(queueCapacity));
        }
        return new GrowingThreadPoolExecutor(numThreads, maxThreads, queueCapacity);
    }

    @Override
    protected Thread newThread(Runnable job) {
        return threadFactory.newThread(() -> {
            CURRENT_POOL.set(this);
            try {
                job.run();
            } finally {
                if (executor instanceof GrowingThreadPoolExecutor growingExecutor) {
                    // this thread has stopped, so a queued task might have no thread to run it
                    growingExecutor.startThreadForQueuedTasks();
                }
            }
        });
    }

    /**
     * Sets the maximum number of threads. Unlike {@link ExecutorThreadPool#setMaxThreads(int)}, it doesn't change
     * the number of threads that a growing pool keeps, which would stop it from growing.
     */
    @Override
    public void setMaxThreads(int threads) {
        if (executor instanceof GrowingThreadPoolExecutor) {
            ThreadPoolBudget budget = getThreadPoolBudget();
            if (budget != null) {
                budget.check(threads);
            }
            executor.setMaximumPoolSize(threads);
        } else {
            super.setMaxThreads(threads);
        }
    }

    /**
     * Sets the time after which an idle thread beyond the number of threads that the pool keeps stops.
     *
     * @param idleTimeout the idle timeout in milliseconds, at least 1
     */
    @Override
    public void setIdleTimeout(int idleTimeout) {
        if (idleTimeout < 1) {
            throw new IllegalArgumentException(
                    "The idle timeout of the HTTP server thread pool must be at least 1 ms, but was " + idleTimeout);
        }
        super.setIdleTimeout(idleTimeout);
    }

    /**
     * Returns whether the calling thread is one of this pool's threads.
     */
    public boolean isPoolThread() {
        return CURRENT_POOL.get() == this;
    }

    @Override
    protected void doStart() throws Exception {
        startAttempted = true;
        super.doStart();
    }

    /**
     * {@link ExecutorThreadPool#join()} waits without a timeout for the executor to terminate, but the executor
     * is shut down only in {@link #doStop()}, which Jetty skips for a pool that was never started. That happens
     * when {@code Server.start()} fails to open a connector (e.g. the port is already in use), since connectors
     * are opened before the server's beans are started. Joining such a pool would block forever.
     */
    @Override
    public void join() throws InterruptedException {
        if (startAttempted) {
            super.join();
        }
    }

    /**
     * A {@link ThreadPoolExecutor} that starts a new thread, up to its maximum pool size, instead of queueing a
     * task when no thread is idle.
     */
    private static final class GrowingThreadPoolExecutor extends ThreadPoolExecutor {
        // a task that only starts a thread, which then runs the queued tasks
        private static final Runnable START_THREAD = () -> { };

        // tasks that have been submitted and haven't completed yet, both queued and running
        private final AtomicInteger submittedTasks = new AtomicInteger();

        GrowingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, int queueCapacity) {
            this(corePoolSize, maximumPoolSize, new TaskQueue(queueCapacity));
        }

        private GrowingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, TaskQueue queue) {
            super(corePoolSize, maximumPoolSize, DEFAULT_IDLE_TIMEOUT_MS, TimeUnit.MILLISECONDS, queue,
                    (task, executor) -> {
                        // the queue declined the task to start a new thread, but the pool reached its maximum
                        // size concurrently, so queue the task after all
                        if (executor.isShutdown() || !queue.forceOffer(task)
                                // the executor could have been shut down and drained before the task was queued
                                || (executor.isShutdown() && queue.remove(task))) {
                            throw new RejectedExecutionException("Task " + task + " rejected from " + executor);
                        }
                    });
            queue.executor = this;
        }

        @Override
        public void execute(Runnable command) {
            submittedTasks.incrementAndGet();
            try {
                super.execute(command);
            } catch (RuntimeException | Error e) {
                submittedTasks.decrementAndGet();
                throw e;
            }
            // The queue decides whether to start a thread from the pool size before it queues the task. A thread
            // that stops after being idle in between doesn't run the task, and ThreadPoolExecutor doesn't replace
            // it while other threads are left, so check again now that the task is queued. Stopping threads check
            // too, after they have left the pool.
            startThreadForQueuedTasks();
        }

        /**
         * Starts a thread if tasks are queued while no thread is idle and the pool is below its maximum size.
         */
        void startThreadForQueuedTasks() {
            if (getQueue().isEmpty() || isShutdown()) {
                return;
            }
            int poolSize = getPoolSize();
            if (poolSize < getMaximumPoolSize() && submittedTasks.get() > poolSize) {
                try {
                    // not counted in submittedTasks, so the queue declines it and the executor starts a thread
                    super.execute(START_THREAD);
                } catch (RejectedExecutionException e) {
                    // the executor is shutting down, or its queue is full, so threads are busy anyway
                }
            }
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            if (r != START_THREAD) {
                submittedTasks.decrementAndGet();
            }
        }
    }

    private static final class TaskQueue extends BlockingArrayQueue<Runnable> {
        private volatile GrowingThreadPoolExecutor executor;

        TaskQueue(int capacity) {
            super(capacity);
        }

        @Override
        public boolean offer(Runnable task) {
            GrowingThreadPoolExecutor executor = this.executor;
            if (executor != null) {
                int poolSize = executor.getPoolSize();
                // returning false makes the executor start a new thread when no thread is idle
                if (poolSize < executor.getMaximumPoolSize() && executor.submittedTasks.get() > poolSize) {
                    return false;
                }
            }
            return super.offer(task);
        }

        boolean forceOffer(Runnable task) {
            return super.offer(task);
        }
    }
}

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
package org.apache.pulsar.broker.service;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker-level memory limiter for replication inflight entries.
 *
 * <p>All replicators on a broker share a total byte budget for in-flight entry data.
 * Before each cursor read, a replicator estimates the bytes it will consume and calls
 * {@link #acquireUpTo(long, Runnable)} to acquire as many bytes as the budget allows.
 * The returned value determines how many entries can be read. If 0 bytes are available,
 * the retry task is enqueued and will be drained later when bytes are released via
 * {@link #release(long)} or {@link #calibrate(long, long)}.
 *
 * <p>The limiter is disabled when {@code maxBytes} is 0.
 */
public class BrokerReplicationMemoryLimiter {

    private final AtomicLong inflightBytes = new AtomicLong(0);
    private volatile long maxBytes;
    private final Queue<PendingTask> pendingTasks = new ConcurrentLinkedQueue<>();
    private final Executor executor;

    public BrokerReplicationMemoryLimiter(Executor executor) {
        this.executor = executor;
        this.maxBytes = 0;
    }

    /**
     * Acquire as many bytes as possible from the budget, up to {@code requestedBytes}.
     * If no bytes are available and a {@code retryTask} is provided, the task is enqueued
     * for later execution when bytes become available.
     *
     * @param requestedBytes the maximum number of bytes the caller would like to acquire
     * @param retryTask      a Runnable to re-trigger the read when bytes are released;
     *                       may be null if the caller will retry through another mechanism.
     *                       Only enqueued when 0 bytes are acquired.
     * @return the actual number of bytes acquired (0 if budget is fully exhausted)
     */
    public long acquireUpTo(long requestedBytes, Runnable retryTask) {
        if (maxBytes <= 0) {
            return requestedBytes;
        }

        while (true) {
            long current = inflightBytes.get();
            long available = maxBytes - current;
            if (available <= 0) {
                if (retryTask != null) {
                    pendingTasks.offer(new PendingTask(requestedBytes, retryTask));
                }
                return 0;
            }
            long acquired = Math.min(available, requestedBytes);
            if (inflightBytes.compareAndSet(current, current + acquired)) {
                return acquired;
            }
        }
    }

    /** @deprecated use {@link #acquireUpTo(long, Runnable)} instead */
    @Deprecated
    public boolean tryAcquire(long estimatedBytes, Runnable retryTask) {
        return acquireUpTo(estimatedBytes, retryTask) >= estimatedBytes;
    }

    /**
     * Release bytes back to the budget after a send completes and the entry is released.
     * Triggers draining of one pending task.
     */
    public void release(long bytes) {
        inflightBytes.addAndGet(-bytes);
        drainPendingTasks();
    }

    /**
     * Calibrate the inflight count by adjusting for the difference between the estimate
     * and the actual byte count. Triggers draining of one pending task.
     */
    public void calibrate(long estimatedBytes, long actualBytes) {
        long delta = actualBytes - estimatedBytes;
        inflightBytes.addAndGet(delta);
        drainPendingTasks();
    }

    /**
     * Release all estimated bytes (when a read that was acquired cannot proceed,
     * e.g., errors during read).
     */
    public void releaseEstimated(long estimatedBytes) {
        inflightBytes.addAndGet(-estimatedBytes);
        drainPendingTasks();
    }

    /**
     * Update the maximum byte budget. Drains pending tasks after the change.
     */
    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
        if (maxBytes > 0) {
            drainPendingTasks();
        }
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public long getInflightBytes() {
        return inflightBytes.get();
    }

    public int getPendingTaskCount() {
        return pendingTasks.size();
    }

    public boolean isLimitReached() {
        long max = maxBytes;
        return max > 0 && inflightBytes.get() >= max;
    }

    private void drainPendingTasks() {
        PendingTask task = pendingTasks.poll();
        if (task != null) {
            executor.execute(task.retryTask);
        }
    }

    private record PendingTask(long estimatedBytes, Runnable retryTask) {
    }
}

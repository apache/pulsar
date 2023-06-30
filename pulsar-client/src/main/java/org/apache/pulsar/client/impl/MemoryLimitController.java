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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryLimitController {

    private final long memoryLimit;
    private final long triggerThreshold;
    private final Runnable trigger;
    private final AtomicLong currentUsage = new AtomicLong();
    private final ReentrantLock mutex = new ReentrantLock(false);
    private final Condition condition = mutex.newCondition();
    private final AtomicBoolean triggerRunning = new AtomicBoolean(false);

    public MemoryLimitController(long memoryLimitBytes) {
        this.memoryLimit = memoryLimitBytes;
        triggerThreshold = 0;
        trigger = null;
    }

    public MemoryLimitController(long memoryLimitBytes, long triggerThreshold, Runnable trigger) {
        this.memoryLimit = memoryLimitBytes;
        this.triggerThreshold = triggerThreshold;
        this.trigger = trigger;
    }

    public void forceReserveMemory(long size) {
        long newUsage = currentUsage.addAndGet(size);
        checkTrigger(newUsage - size, newUsage);
    }

    public boolean tryReserveMemory(long size) {
        while (true) {
            long current = currentUsage.get();
            long newUsage = current + size;

            // We allow one request to go over the limit, to make the notification
            // path simpler and more efficient
            if (current > memoryLimit && memoryLimit > 0) {
                return false;
            }

            if (currentUsage.compareAndSet(current, newUsage)) {
                checkTrigger(current, newUsage);
                return true;
            }
        }
    }

    private void checkTrigger(long prevUsage, long newUsage) {
        if (newUsage >= triggerThreshold && prevUsage < triggerThreshold && trigger != null) {
            if (triggerRunning.compareAndSet(false, true)) {
                try {
                    trigger.run();
                } finally {
                    triggerRunning.set(false);
                }
            }
        }
    }

    public void reserveMemory(long size) throws InterruptedException {
        if (!tryReserveMemory(size)) {
            mutex.lock();
            try {
                while (!tryReserveMemory(size)) {
                    condition.await();
                }
            } finally {
                mutex.unlock();
            }
        }
    }

    public void releaseMemory(long size) {
        long newUsage = currentUsage.addAndGet(-size);
        if (newUsage + size > memoryLimit
                && newUsage <= memoryLimit) {
            // We just crossed the limit. Now we have more space
            mutex.lock();
            try {
                condition.signalAll();
            } finally {
                mutex.unlock();
            }
        }
    }

    public long currentUsage() {
        return currentUsage.get();
    }

    public double currentUsagePercent() {
        return 1.0 * currentUsage.get() / memoryLimit;
    }

    public boolean isMemoryLimited() {
        return memoryLimit > 0;
    }
}

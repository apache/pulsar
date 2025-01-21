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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Executor that runs tasks in the current thread when
 * there aren't any critical sections in execution.
 * If there's a critical section in execution, the tasks are queued
 * and postponed until all critical sections have been exited.
 * The tasks are run on the thread that exited the last critical section.
 */
public class OutsideCriticalSectionsExecutor implements Executor {
    private final AtomicInteger criticalSectionsCount = new AtomicInteger();
    private final Queue<Runnable> queuedTasks = new ConcurrentLinkedQueue<>();
    private final ReadWriteLock executionLock = new ReentrantReadWriteLock();

    /**
     * Executes the given command at some time in the future.
     * If there are no critical sections in execution, the command is executed immediately.
     * If there are critical sections in execution, the command is queued and executed after all critical sections have
     * been exited.
     */
    @Override
    public void execute(Runnable command) {
        executionLock.writeLock().lock();
        try {
            if (criticalSectionsCount.get() == 0) {
                command.run();
            } else {
                queuedTasks.add(command);
            }
        } finally {
            executionLock.writeLock().unlock();
        }
    }

    /**
     * Enters a critical section. This method should be called before entering a critical section.
     */
    public void enterCriticalSection() {
        executionLock.readLock().lock();
        try {
            criticalSectionsCount.incrementAndGet();
        } finally {
            executionLock.readLock().unlock();
        }
    }

    /**
     * Exits a critical section. This method should be called after exiting a critical section.
     */
    public void exitCriticalSection() {
        if (criticalSectionsCount.decrementAndGet() == 0) {
            runQueuedTasks();
        }
    }

    /**
     * Runs a callable which is a critical section. This method should be used when
     * the result of the callable is needed and it should run as a critical section.
     */
    public <T> T runCriticalSectionCallable(Callable<T> callable) {
        executionLock.readLock().lock();
        try {
            criticalSectionsCount.incrementAndGet();
            return callable.call();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        } finally {
            executionLock.readLock().unlock();
            exitCriticalSection();
        }
    }

    private void runQueuedTasks() {
        executionLock.writeLock().lock();
        try {
            if (criticalSectionsCount.get() != 0) {
                return;
            }
            Runnable command;
            while ((command = queuedTasks.poll()) != null) {
                command.run();
            }
        } finally {
            executionLock.writeLock().unlock();
        }
    }
}

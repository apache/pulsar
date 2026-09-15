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
package org.apache.pulsar.client.util;

import com.google.common.annotations.VisibleForTesting;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Keeps automatic shutdown when the owner no longer retains the executor facade. */
final class AutoShutdownExecutorService extends AbstractExecutorService {
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

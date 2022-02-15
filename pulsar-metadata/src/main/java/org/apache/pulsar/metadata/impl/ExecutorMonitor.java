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
package org.apache.pulsar.metadata.impl;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutorMonitor implements AutoCloseable {

    private final ExecutorService executorToMonitor;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> task;
    private final long timeoutSeconds;
    private final Consumer<Long> consumer;

    public ExecutorMonitor(ExecutorService executor, long timeoutSeconds, Consumer<Long> consumer) {
        this.executorToMonitor = executor;
        this.scheduler = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("metadata-store-executor-monitor"));
        this.task = scheduler.scheduleAtFixedRate(this::executeTask, 1000, 5000, TimeUnit.MILLISECONDS);
        this.timeoutSeconds = timeoutSeconds;
        this.consumer = consumer;
    }

    private synchronized void executeTask() {
        Future<?> f = executorToMonitor.submit(() -> {
            if (log.isDebugEnabled()) {
                log.debug("Executor callback invoked on thread {}, {}", Thread.currentThread().getId(),
                  Thread.currentThread().getName());
                return;
            }
        });
        long start = System.currentTimeMillis();
        try {
            f.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Got exception waiting for executor callback", e);
            long elapsed = System.currentTimeMillis() - start;
            consumer.accept(elapsed);
        }
    }

    @Override
    public void close() throws Exception {
        task.cancel(true);
        scheduler.shutdownNow();
        scheduler.awaitTermination(10, TimeUnit.SECONDS);
    }
}

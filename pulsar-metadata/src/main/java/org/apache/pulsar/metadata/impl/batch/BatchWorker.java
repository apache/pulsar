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

package org.apache.pulsar.metadata.impl.batch;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pulsar.metadata.api.MetadataStoreException;

public class BatchWorker {
    private final Consumer<List<MetadataOp<?>>> batchHandler;
    private volatile long lastBatchOpsExecuteTime;
    protected final int maxDelayMillis;
    protected final int maxOpsPerBatch;
    protected Queue<MetadataOp<?>> opQueue;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running;

    public BatchWorker(String name,
                       int maxDelayMillis, int maxOpsPerBatch,
                       Consumer<List<MetadataOp<?>>> batchHandler) {
        this.maxDelayMillis = maxDelayMillis;
        this.maxOpsPerBatch = maxOpsPerBatch;
        this.opQueue = new ConcurrentLinkedQueue<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("metadata-store-batch-worker-" + name));
        scheduler.scheduleWithFixedDelay(this::doBatch, maxDelayMillis, maxDelayMillis, TimeUnit.MILLISECONDS);
        this.batchHandler = batchHandler;
        this.running = true;
    }

    private boolean isBatchReady() {
        if (!running) {
            return false;
        }
        return opQueue.size() > maxOpsPerBatch
                || (System.currentTimeMillis() - lastBatchOpsExecuteTime) > maxDelayMillis;
    }

    private void triggerBatch() {
        if (isBatchReady()) {
            scheduler.execute(this::doBatch);
        }
    }

    private void doBatch() {
        while (running && isBatchReady()) {
            lastBatchOpsExecuteTime = System.currentTimeMillis();
            List<MetadataOp<?>> singleBatch = new ArrayList<>();
            while (singleBatch.size() < maxOpsPerBatch) {
                if (!opQueue.isEmpty()) {
                    singleBatch.add(opQueue.poll());
                } else {
                    break;
                }
            }
            if (singleBatch.isEmpty()) {
                break;
            }
            batchHandler.accept(singleBatch);
        }
    }

    public boolean add(MetadataOp<?> op) {
        if (!running) {
            return false;
        }
        if (opQueue.offer(op)) {
            triggerBatch();
            return true;
        }
        return false;
    }

    public void close() {
        synchronized (this) {
            if (!running) {
                return;
            }
            running = false;
        }
        while (!opQueue.isEmpty()) {
            MetadataOp<?> op = opQueue.poll();
            op.getFuture().completeExceptionally(
                    new MetadataStoreException.AlreadyClosedException("Metadata store already closed."));
        }
        scheduler.shutdown();
    }
}

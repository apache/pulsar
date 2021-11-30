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
package org.apache.pulsar.metadata.impl.batching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;

@Slf4j
public abstract class AbstractBatchedMetadataStore extends AbstractMetadataStore {

    private final ScheduledFuture<?> scheduledTask;
    private final MessagePassingQueue<MetadataOp> readOps;
    private final MessagePassingQueue<MetadataOp> writeOps;

    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);

    private final boolean enabled;
    private final int maxDelayMillis;
    private final int maxOperations;
    private final int maxSize;

    protected AbstractBatchedMetadataStore(MetadataStoreConfig conf) {
        super();

        this.enabled = conf.isBatchingEnabled();
        this.maxDelayMillis = conf.getBatchingMaxDelayMillis();
        this.maxOperations = conf.getBatchingMaxOperations();
        this.maxSize = conf.getBatchingMaxSizeKb() * 1_024;

        if (enabled) {
            readOps = new MpscUnboundedArrayQueue<>(10_000);
            writeOps = new MpscUnboundedArrayQueue<>(10_000);
            scheduledTask =
                    executor.scheduleAtFixedRate(this::flush, maxDelayMillis, maxDelayMillis, TimeUnit.MILLISECONDS);
        } else {
            scheduledTask = null;
            readOps = null;
            writeOps = null;
        }
    }

    @Override
    public void close() throws Exception {
        if (enabled) {
            // Fail all the pending items
            Exception ex = new IllegalStateException("Metadata store is getting closed");
            readOps.drain(op -> op.getFuture().completeExceptionally(ex));
            writeOps.drain(op -> op.getFuture().completeExceptionally(ex));

            scheduledTask.cancel(true);
        }
        super.close();
    }

    private void flush() {
        while (!readOps.isEmpty()) {
            List<MetadataOp> ops = new ArrayList<>();
            readOps.drain(ops::add, maxOperations);
            batchOperation(ops);
        }

        while (!writeOps.isEmpty()) {
            int batchSize = 0;

            List<MetadataOp> ops = new ArrayList<>();
            for (int i = 0; i < maxOperations; i++) {
                MetadataOp op = writeOps.peek();
                if (op == null) {
                    break;
                }

                if (i > 0 && (batchSize + op.size()) > maxSize) {
                    // We have already reached the max size, so flush the current batch
                    break;
                }

                batchSize += op.size();
                ops.add(writeOps.poll());
            }
            batchOperation(ops);
        }

        flushInProgress.set(false);
    }

    @Override
    public final CompletableFuture<Optional<GetResult>> storeGet(String path) {
        OpGet op = new OpGet(path);
        enqueue(readOps, op);
        return op.getFuture();
    }

    @Override
    protected final CompletableFuture<List<String>> getChildrenFromStore(String path) {
        OpGetChildren op = new OpGetChildren(path);
        enqueue(readOps, op);
        return op.getFuture();
    }

    @Override
    protected final CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
        OpDelete op = new OpDelete(path, expectedVersion);
        enqueue(writeOps, op);
        return op.getFuture();
    }

    @Override
    protected final CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                               EnumSet<CreateOption> options) {
        OpPut op = new OpPut(path, data, optExpectedVersion, options);
        enqueue(writeOps, op);
        return op.getFuture();
    }

    private void enqueue(MessagePassingQueue queue, MetadataOp op) {
        if (enabled) {
            if (!queue.offer(op)) {
                // Execute individually if we're failing to enqueue
                batchOperation(Collections.singletonList(op));
                return;
            }
            if (queue.size() > maxOperations && flushInProgress.compareAndSet(false, true)) {
                executor.execute(this::flush);
            }
        } else {
            batchOperation(Collections.singletonList(op));
        }
    }

    protected abstract void batchOperation(List<MetadataOp> ops);
}

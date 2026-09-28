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

package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.jctools.queues.MpscUnboundedArrayQueue;

@CustomLog
class BatchingHandoverExecutorWrapper implements Executor {
    private final Executor delegate;
    private final int handoverMaxBatchSize;
    private final Consumer<Throwable> runnableFailureProcessor;
    private final AtomicBoolean handoverScheduled = new AtomicBoolean();
    private final MpscUnboundedArrayQueue<Runnable> handoverQueue;

    BatchingHandoverExecutorWrapper(Executor delegate, int queueChunkSize, int handoverMaxBatchSize,
                                           Consumer<Throwable> runnableFailureProcessor) {
        this.delegate = delegate;
        this.handoverMaxBatchSize = handoverMaxBatchSize;
        this.runnableFailureProcessor = runnableFailureProcessor;
        this.handoverQueue = new MpscUnboundedArrayQueue<>(queueChunkSize);
    }

    @Override
    public void execute(Runnable command) {
        if (handoverMaxBatchSize == 0) {
            delegate.execute(command);
            return;
        }
        handoverQueue.offer(command);
        scheduleHandover();
    }

    private void scheduleHandover() {
        if (handoverScheduled.compareAndSet(false, true)) {
            try {
                delegate.execute(this::runHandoverBatch);
            } catch (RuntimeException e) {
                // Let a later add retry scheduling, and fail this caller like a rejected task did before.
                handoverScheduled.set(false);
                throw e;
            }
        }
    }

    private void runHandoverBatch() {
        // Clear the flag before polling: an add queued after this point schedules another batch if this one misses
        // it, so no add is left behind.
        handoverScheduled.set(false);

        Runnable command;
        for (int i = 0; i < handoverMaxBatchSize && (command = handoverQueue.relaxedPoll()) != null; i++) {
            try {
                command.run();
            } catch (Throwable t) {
                runnableFailureProcessor.accept(t);
            }
        }

        if (!handoverQueue.isEmpty()) {
            // Leave the rest to the next handover batch so that other executor tasks, such as add completions, can run.
            scheduleHandover();
        }
    }
}

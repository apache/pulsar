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

/**
 * An {@link Executor} that hands tasks over to a delegate executor in batches instead of one task at a time.
 *
 * <p>Submitting threads append tasks to a multi-producer single-consumer queue. The thread that finds no handover batch
 * scheduled submits one task to the delegate, and that task runs up to {@code handoverMaxBatchSize} of the queued
 * tasks. The delegate's own queue then sees one task per batch rather than one per submitted task, so concurrent
 * submitters contend on it once per batch, and other work on the delegate does not wait behind a task per submission.
 * When more tasks remain after a batch, the next batch is submitted to the delegate as a new task, so that tasks
 * submitted to the delegate directly can run in between.
 *
 * <p>Tasks submitted by one thread run in the order that thread submitted them. The delegate must run the tasks
 * submitted to it one at a time, such as a single-threaded executor, since the queue supports a single consumer.
 *
 * <p>A {@code handoverMaxBatchSize} of 0 disables batching: each task is then submitted to the delegate on its own.
 */
@CustomLog
class BatchingExecutorWrapper implements Executor {
    private final Executor delegate;
    private final int handoverMaxBatchSize;
    private final Consumer<Throwable> runnableFailureProcessor;
    private final AtomicBoolean handoverScheduled = new AtomicBoolean();
    private final MpscUnboundedArrayQueue<Runnable> handoverQueue;

    /**
     * Creates a wrapper that hands tasks over to {@code delegate} in batches.
     *
     * @param delegate the executor that runs the handover batches; it must run its tasks one at a time
     * @param queueChunkSize the chunk size of the handover queue, which grows by linking chunks of this size
     * @param handoverMaxBatchSize the maximum number of tasks run by one handover batch, or 0 to disable batching
     * @param runnableFailureProcessor receives what a task run by a handover batch throws, so that the remaining
     *                                 tasks of the batch still run
     */
    BatchingExecutorWrapper(Executor delegate, int queueChunkSize, int handoverMaxBatchSize,
                            Consumer<Throwable> runnableFailureProcessor) {
        this.delegate = delegate;
        this.handoverMaxBatchSize = handoverMaxBatchSize;
        this.runnableFailureProcessor = runnableFailureProcessor;
        this.handoverQueue = new MpscUnboundedArrayQueue<>(queueChunkSize);
    }

    /**
     * Queues {@code command} for the next handover batch, submitting that batch to the delegate unless it is already
     * scheduled. With batching disabled, submits {@code command} to the delegate directly.
     *
     * @throws RuntimeException what the delegate throws when it rejects the handover batch, such as
     *                          {@code RejectedExecutionException}
     */
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
        // Clear the flag before polling: a task queued after this point schedules another batch if this one misses
        // it, so no task is left behind.
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
            // Leave the rest to the next handover batch so that other tasks on the delegate can run in between.
            scheduleHandover();
        }
    }
}

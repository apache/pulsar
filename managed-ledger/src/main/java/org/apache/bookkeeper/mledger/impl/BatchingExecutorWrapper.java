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

import static com.google.common.base.Preconditions.checkArgument;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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
 * <p>Batching needs a {@code handoverMaxBatchSize} greater than 1. To hand tasks over one at a time, submit them to
 * the delegate directly instead of wrapping it.
 */
class BatchingExecutorWrapper implements Executor {
    private final Executor delegate;
    private final int handoverMaxBatchSize;
    private final Consumer<Throwable> runFailureConsumer;
    private final AtomicBoolean handoverScheduled = new AtomicBoolean();
    private final MpscUnboundedArrayQueue<Runnable> handoverQueue;

    /**
     * Creates a wrapper that hands tasks over to {@code delegate} in batches.
     *
     * @param delegate the executor that runs the handover batches; it must run its tasks one at a time
     * @param queueChunkSize the chunk size of the handover queue, which grows by linking chunks of this size
     * @param handoverMaxBatchSize the maximum number of tasks run by one handover batch; must be greater than 1
     * @param runFailureConsumer receives what a task run by a handover batch throws, so that the remaining
     *                                 tasks of the batch still run
     */
    BatchingExecutorWrapper(Executor delegate, int queueChunkSize, int handoverMaxBatchSize,
                            Consumer<Throwable> runFailureConsumer) {
        checkArgument(handoverMaxBatchSize > 1, "handoverMaxBatchSize must be greater than 1");
        this.delegate = delegate;
        this.handoverMaxBatchSize = handoverMaxBatchSize;
        this.runFailureConsumer = runFailureConsumer;
        this.handoverQueue = new MpscUnboundedArrayQueue<>(queueChunkSize);
    }

    /**
     * Queues {@code command} for the next handover batch, submitting that batch to the delegate unless it is already
     * scheduled.
     *
     * @throws RuntimeException what the delegate throws when it rejects the handover batch, such as
     *                          {@code RejectedExecutionException}
     * @throws IllegalArgumentException if {@code handoverMaxBatchSize} is not greater than 1
     */
    @Override
    public void execute(Runnable command) {
        handoverQueue.offer(command);
        scheduleHandover();
    }

    private void scheduleHandover() {
        if (handoverScheduled.compareAndSet(false, true)) {
            try {
                delegate.execute(this::runHandoverBatch);
            } catch (RuntimeException e) {
                // Let a later task retry scheduling, and fail this caller as the delegate would have.
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
                runFailureConsumer.accept(t);
            }
        }

        if (!handoverQueue.isEmpty()) {
            // Leave the rest to the next handover batch so that other tasks on the delegate can run in between.
            scheduleHandover();
        }
    }
}

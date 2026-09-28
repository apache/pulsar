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
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.jspecify.annotations.Nullable;

/**
 * An {@link Executor} that hands tasks over to a delegate executor in batches instead of one task at a time.
 *
 * <p>Submitting threads append tasks to a multi-producer single-consumer queue. The thread that finds no handover batch
 * scheduled submits one task to the delegate, and that task runs the queued tasks until it has run {@code maxItems}
 * tasks or their weights add up to {@code maxWeight}. The delegate's own queue then sees one task per batch rather than
 * one per submitted task, so concurrent submitters contend on it once per batch, and other work on the delegate does
 * not wait behind a task per submission. When more tasks remain after a batch, the next batch is submitted to the
 * delegate as a new task, so that tasks submitted to the delegate directly can run in between.
 *
 * <p>The weight limit keeps a batch of heavy tasks from occupying the delegate for as long as {@code maxItems} of
 * them would, so that the other work sharing the delegate is not starved. A task that implements
 * {@link WeightedRunnable} weighs {@link WeightedRunnable#getWeight()}; any other task weighs 0 and is limited only by
 * {@code maxItems}. A batch always runs at least one task, even one that weighs more than {@code maxWeight}.
 *
 * <p>Tasks submitted by one thread run in the order that thread submitted them. The delegate must run the tasks
 * submitted to it one at a time, such as a single-threaded executor, since the queue supports a single consumer.
 *
 * <p>When the delegate rejects a handover batch, {@link #execute(Runnable)} throws what the delegate threw if the
 * task it submitted had not run yet, and every other queued task is passed to the rejected task handler instead of
 * being left in the queue.
 *
 * <p>The queue is created by the first submitted task, so that a wrapper that is never used does not allocate it.
 *
 * <p>Batching needs a {@code maxItems} greater than 1. To hand tasks over one at a time, submit them to the delegate
 * directly instead of wrapping it.
 */
class BatchingExecutorWrapper implements Executor {
    /**
     * A task with a weight, such as the number of bytes it processes, which counts towards the {@code maxWeight} of
     * the handover batch that runs it.
     */
    interface WeightedRunnable extends Runnable {
        /**
         * Returns the weight of this task. It is read before the task runs.
         */
        long getWeight();
    }

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<BatchingExecutorWrapper, MpscUnboundedArrayQueue>
            HANDOVER_QUEUE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(BatchingExecutorWrapper.class,
                    MpscUnboundedArrayQueue.class, "handoverQueue");

    private final Executor delegate;
    private final int queueChunkSize;
    private final int maxItems;
    private final long maxWeight;
    private final Consumer<Throwable> runFailureConsumer;
    private final BiConsumer<Runnable, RuntimeException> rejectedTaskHandler;
    // Set while a handover batch is scheduled or running. The thread that sets it is the only one that takes tasks
    // from the handover queue until it clears it, which keeps the queue single-consumer.
    private final AtomicBoolean handoverScheduled = new AtomicBoolean();
    // Created by the first task, and never replaced.
    private volatile MpscUnboundedArrayQueue<Runnable> handoverQueue;

    /**
     * Creates a wrapper that hands tasks over to {@code delegate} in batches.
     *
     * @param delegate the executor that runs the handover batches; it must run its tasks one at a time
     * @param queueChunkSize the chunk size of the handover queue, which grows by linking chunks of this size
     * @param maxItems the maximum number of tasks run by one handover batch; must be greater than 1
     * @param maxWeight the total weight of tasks after which a handover batch stops taking more tasks; must be
     *                  positive, and {@link Long#MAX_VALUE} leaves batches limited only by {@code maxItems}
     * @param runFailureConsumer receives what a task run by a handover batch, or the rejected task handler, throws,
     *                           so that the remaining tasks still run
     * @param rejectedTaskHandler receives each queued task that will not run because the delegate rejected its
     *                            handover batch, with what the delegate threw; it runs on the thread whose
     *                            submission was rejected
     * @throws IllegalArgumentException if {@code maxItems} is not greater than 1 or {@code maxWeight} is not positive
     */
    BatchingExecutorWrapper(Executor delegate, int queueChunkSize, int maxItems, long maxWeight,
                            Consumer<Throwable> runFailureConsumer,
                            BiConsumer<Runnable, RuntimeException> rejectedTaskHandler) {
        checkArgument(maxItems > 1, "maxItems must be greater than 1");
        checkArgument(maxWeight > 0, "maxWeight must be positive");
        this.delegate = delegate;
        this.queueChunkSize = queueChunkSize;
        this.maxItems = maxItems;
        this.maxWeight = maxWeight;
        this.runFailureConsumer = runFailureConsumer;
        this.rejectedTaskHandler = rejectedTaskHandler;
    }

    /**
     * Queues {@code command} for the next handover batch, submitting that batch to the delegate unless it is already
     * scheduled.
     *
     * @throws RuntimeException what the delegate throws when it rejects the handover batch before {@code command} ran,
     *                          such as {@code RejectedExecutionException}; {@code command} then does not run
     */
    @Override
    public void execute(Runnable command) {
        handoverQueue().offer(command);
        scheduleHandover(command);
    }

    @SuppressWarnings("unchecked")
    private MpscUnboundedArrayQueue<Runnable> handoverQueue() {
        MpscUnboundedArrayQueue<Runnable> queue = handoverQueue;
        if (queue == null) {
            queue = new MpscUnboundedArrayQueue<>(queueChunkSize);
            if (!HANDOVER_QUEUE_UPDATER.compareAndSet(this, null, queue)) {
                // Another submitting thread created it first: every thread must use the same queue.
                queue = handoverQueue;
            }
        }
        return queue;
    }

    @VisibleForTesting
    boolean hasHandoverQueue() {
        return handoverQueue != null;
    }

    @VisibleForTesting
    int getMaxItems() {
        return maxItems;
    }

    @VisibleForTesting
    long getMaxWeight() {
        return maxWeight;
    }

    /**
     * Submits a handover batch to the delegate unless one is already scheduled.
     *
     * @param submitted the task the calling thread just queued, or null when called by a handover batch
     */
    private void scheduleHandover(@Nullable Runnable submitted) {
        RuntimeException submittedRejection = null;
        while (handoverScheduled.compareAndSet(false, true)) {
            try {
                delegate.execute(this::runHandoverBatch);
                break;
            } catch (RuntimeException e) {
                if (rejectQueuedTasks(submitted, e)) {
                    submittedRejection = e;
                    submitted = null;
                }
                handoverScheduled.set(false);
                // Retry for a task queued after the queue was emptied: its thread found the flag set and returned.
                if (handoverQueue.isEmpty()) {
                    break;
                }
            }
        }
        if (submittedRejection != null) {
            throw submittedRejection;
        }
    }

    /**
     * Takes every queued task and passes it to the rejected task handler, except {@code submitted}, which the caller
     * fails itself. The calling thread must have set the handover flag.
     *
     * @return whether {@code submitted} was still queued
     */
    private boolean rejectQueuedTasks(@Nullable Runnable submitted, RuntimeException rejection) {
        boolean submittedQueued = false;
        Runnable task;
        // poll() rather than relaxedPoll(): a task whose submission completed must be seen, even if a task queued
        // before it is still being written.
        while ((task = handoverQueue.poll()) != null) {
            if (task == submitted && !submittedQueued) {
                submittedQueued = true;
                continue;
            }
            try {
                rejectedTaskHandler.accept(task, rejection);
            } catch (Throwable t) {
                runFailureConsumer.accept(t);
            }
        }
        return submittedQueued;
    }

    private void runHandoverBatch() {
        MpscUnboundedArrayQueue<Runnable> queue = handoverQueue;
        int items = 0;
        long weight = 0;
        Runnable command;
        while (items < maxItems && weight < maxWeight && (command = queue.relaxedPoll()) != null) {
            items++;
            try {
                if (command instanceof WeightedRunnable weightedCommand) {
                    weight += weightedCommand.getWeight();
                }
                command.run();
            } catch (Throwable t) {
                runFailureConsumer.accept(t);
            }
        }

        // Clear the flag before checking the queue: a task queued while the flag was set did not schedule a batch, so
        // it is picked up here. Tasks left over by the limits are handed to a new batch, so that other tasks on the
        // delegate can run in between.
        handoverScheduled.set(false);
        if (!queue.isEmpty()) {
            scheduleHandover(null);
        }
    }
}

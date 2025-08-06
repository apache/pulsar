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
package org.apache.pulsar.common.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SingleThreadSafeScheduledExecutorService extends ScheduledThreadPoolExecutor
        implements ScheduledExecutorService {

    private static final AtomicLong fixRateTaskSequencerGenerator = new AtomicLong();

    private static final RejectedExecutionHandler defaultRejectedExecutionHandler = new AbortPolicy();

    private volatile RejectedExecutionHandler rejectedExecutionHandler = defaultRejectedExecutionHandler;

    public SingleThreadSafeScheduledExecutorService(String name) {
        super(1, new DefaultThreadFactory(name));
        super.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        super.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        super.setRemoveOnCancelPolicy(false);
        // Trigger worker thread creation.
        submit(() -> {});
    }

    private static final class SafeRunnable implements Runnable {
        private final Runnable task;

        SafeRunnable(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            try {
                task.run();
            } catch (Throwable t) {
                log.warn("Unexpected throwable from task {}: {}", task.getClass(), t.getMessage(), t);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return super.schedule(new SafeRunnable(command), delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                     long initialDelay, long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(new SafeRunnable(command), initialDelay, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                  long initialDelay, long period, TimeUnit unit) {
        return super.scheduleAtFixedRate(new SafeRunnable(command), initialDelay, period, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<?> submit(Runnable task) {
        return super.submit(new SafeRunnable(task));
    }

    /***
     * Different with {@link #scheduleAtFixedRate(Runnable, long, long, TimeUnit)}, If the execution time of the next
     * period task > period: New tasks will trigger be dropped, instead, execute the next period task after the current
     * time.
     */
    public ScheduledFuture<?> scheduleAtFixedRateAndDropOutdatedTask(Runnable command,
                                                                     long initialDelay,
                                                                     long period,
                                                                     TimeUnit unit) {
        if (command == null || unit == null)
            throw new NullPointerException();
        if (period <= 0L)
            throw new IllegalArgumentException("period can not be null");
        ScheduledFutureTask<Void> sft =
                new ScheduledFutureTask<Void>(command,
                        null,
                        triggerTime(initialDelay, unit),
                        unit.toNanos(period),
                        fixRateTaskSequencerGenerator.getAndIncrement());
        RunnableScheduledFuture<Void> t = decorateTask(command, sft);
        sft.outerTask = t;
        delayedExecute(t);
        return t;
    }

    /**
     * Main execution method for delayed or periodic tasks.  If pool
     * is shut down, rejects the task. Otherwise adds task to queue
     * and starts a thread, if necessary, to run it.  (We cannot
     * prestart the thread to run the task because the task (probably)
     * shouldn't be run yet.)  If the pool is shut down while the task
     * is being added, cancel and remove it if required by state and
     * run-after-shutdown parameters.
     *
     * @param task the task
     */
    private void delayedExecute(RunnableScheduledFuture<?> task) {
        if (isShutdown())
            reject(task);
        else {
            super.getQueue().add(task);
            if (!canRunInCurrentRunState(task) && remove(task)) {
                task.cancel(false);
            }
        }
    }

    /**
     * Invokes the rejected execution handler for the given command.
     * Package-protected for use by ScheduledThreadPoolExecutor.
     */
    private void reject(Runnable command) {
        rejectedExecutionHandler.rejectedExecution(command, this);
    }

    /**
     * Returns the nanoTime-based trigger time of a delayed action.
     */
    private long triggerTime(long delay, TimeUnit unit) {
        return triggerTime(unit.toNanos((delay < 0) ? 0 : delay));
    }

    /**
     * Returns the nanoTime-based trigger time of a delayed action.
     */
    private long triggerTime(long delay) {
        return System.nanoTime() +
                ((delay < (Long.MAX_VALUE >> 1)) ? delay : overflowFree(delay));
    }

    /**
     * Constrains the values of all delays in the queue to be within
     * Long.MAX_VALUE of each other, to avoid overflow in compareTo.
     * This may occur if a task is eligible to be dequeued, but has
     * not yet been, while some other task is added with a delay of
     * Long.MAX_VALUE.
     */
    private long overflowFree(long delay) {
        Delayed head = (Delayed) super.getQueue().peek();
        if (head != null) {
            long headDelay = head.getDelay(NANOSECONDS);
            if (headDelay < 0 && (delay - headDelay < 0))
                delay = Long.MAX_VALUE + headDelay;
        }
        return delay;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setContinueExistingPeriodicTasksAfterShutdownPolicy(boolean flag) {
        throw new IllegalArgumentException("Not support to set continueExistingPeriodicTasksAfterShutdownPolicy, it is"
                + " always false");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getContinueExistingPeriodicTasksAfterShutdownPolicy() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setExecuteExistingDelayedTasksAfterShutdownPolicy(boolean flag) {
        throw new IllegalArgumentException("Not support to set executeExistingDelayedTasksAfterShutdownPolicy, it is"
                + " always false");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getExecuteExistingDelayedTasksAfterShutdownPolicy() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRemoveOnCancelPolicy(boolean value) {
        throw new IllegalArgumentException("Not support to set removeOnCancelPolicy, it is always false");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getRemoveOnCancelPolicy() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
        super.setRejectedExecutionHandler(handler);
        this.rejectedExecutionHandler = handler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RejectedExecutionHandler getRejectedExecutionHandler() {
        return rejectedExecutionHandler;
    }

    private class ScheduledFutureTask<V>
            extends FutureTask<V> implements RunnableScheduledFuture<V> {

        /** Sequence number to break ties FIFO */
        private final long sequenceNumber;

        /** The nanoTime-based time when the task is enabled to execute. */
        private volatile long time;

        /**
         * Period for repeating tasks, in nanoseconds.
         * A positive value indicates fixed-rate execution.
         * A negative value indicates fixed-delay execution.
         * A value of 0 indicates a non-repeating (one-shot) task.
         */
        private final long period;

        /** The actual task to be re-enqueued by reExecutePeriodic */
        RunnableScheduledFuture<V> outerTask = this;

        /**
         * Creates a periodic action with given nanoTime-based initial
         * trigger time and period.
         */
        ScheduledFutureTask(Runnable r, V result, long triggerTime,
                            long period, long sequenceNumber) {
            super(r, result);
            this.time = triggerTime;
            this.period = period;
            this.sequenceNumber = sequenceNumber;
        }

        public long getDelay(TimeUnit unit) {
            return unit.convert(time - System.nanoTime(), NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            if (other == this) // compare zero if same object
                return 0;
            if (other instanceof ScheduledFutureTask) {
                ScheduledFutureTask<?>
                        x = (ScheduledFutureTask<?>)other;
                long diff = time - x.time;
                if (diff < 0)
                    return -1;
                else if (diff > 0)
                    return 1;
                else if (sequenceNumber < x.sequenceNumber)
                    return -1;
                else
                    return 1;
            }
            long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
            return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
        }

        /**
         * Returns {@code true} if this is a periodic (not a one-shot) action.
         */
        public boolean isPeriodic() {
            return period != 0;
        }

        /**
         * Sets the next time to run for a periodic task.
         */
        private void setNextRunTime() {
            // If the execution time of the next period task > period: New tasks will trigger be dropped, instead,
            // execute the next period task after the current time.
            if (System.nanoTime() > time + period) {
                time += ((System.nanoTime() - time) / period + 1) * period;
            } else {
                time += period;
            }
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            // The racy read of heapIndex below is benign:
            // if heapIndex < 0, then OOTA guarantees that we have surely
            // been removed; else we recheck under lock in remove()
            return super.cancel(mayInterruptIfRunning);
        }

        /**
         * Overrides FutureTask version so as to reset/requeue if periodic.
         */
        public void run() {
            if (!canRunInCurrentRunState(this))
                cancel(false);
            else if (!isPeriodic())
                super.run();
            else if (super.runAndReset()) {
                setNextRunTime();
                reExecutePeriodic(outerTask);
            }
        }
    }

    /**
     * Re-queues a periodic task unless current run state precludes it.
     * Same idea as delayedExecute except drops task rather than rejecting.
     *
     * @param task the task
     */
    private void reExecutePeriodic(RunnableScheduledFuture<?> task) {
        if (canRunInCurrentRunState(task)) {
            super.getQueue().add(task);
            return;
        }
        task.cancel(false);
    }

    /**
     * Returns true if can run a task given current run state and
     * run-after-shutdown parameters.
     */
    private boolean canRunInCurrentRunState(RunnableScheduledFuture<?> task) {
        // Since the policies "continueExistingPeriodicTasksAfterShutdownPolicy" and
        // "executeExistingDelayedTasksAfterShutdownPolicy" are always "false", the checking of "terminating" is not
        // needed.
        return !isShutdown();
    }
}

package org.apache.pulsar.client.impl;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Used to make all tasks that will modify subscriptions will be executed one by one, and skip the unnecessary updating.
 *
 * So far, four three scenarios that will modify subscriptions:
 * 1. When start pattern consumer.
 * 2. After topic list watcher reconnected, it will call {@link PatternMultiTopicsConsumerImpl#recheckTopicsChange()}.
 *    this scenario only exists in the version >= 2.11 (both client-version and broker version are >= 2.11).
 * 3. A scheduled task will call {@link PatternMultiTopicsConsumerImpl#recheckTopicsChange()}, this scenario only
 *    exists in the version < 2.11.
 * 4. The topics change events will trigger a
 *    {@link PatternMultiTopicsConsumerImpl#topicsChangeListener#onTopicsRemoved(Collection)} or
 *    {@link PatternMultiTopicsConsumerImpl#topicsChangeListener#onTopicsAdded(Collection)}.
 *
 * When you are using a release >= 2.11, there are three scenarios: [1, 2, 4].
 * When you are using a release < 2.11, there is only one scenario: [3] and all the event will run in the same thread.
 */
public class PatternConsumerUpdateQueue {

    private static final Pair<UpdateSubscriptionType, Collection<String>> RECHECK_OP =
            Pair.of(UpdateSubscriptionType.RECHECK, null);

    private final LinkedBlockingQueue<Pair<UpdateSubscriptionType, Collection<String>>> pendingTasks;

    private final PatternMultiTopicsConsumerImpl patternConsumer;

    private final PatternMultiTopicsConsumerImpl.TopicsChangedListener topicsChangeListener;

    /**
     * Whether there is a task is in progress, this variable is used to confirm whether a next-task triggering is
     * needed.
     */
    private CompletableFuture<Void> taskInProgress = null;

    /**
     * Whether there is a recheck task in queue.
     * - Since recheck task will do all changes, it can be used to compress multiple tasks to one.
     * - To avoid skipping the newest changes, once the recheck task is starting to work, this variable will be set
     *   to "false".
     */
    private boolean recheckTaskInQueue = false;

    private long lastRecheckTaskStartingTimestamp = 0;

    private boolean closed;

    public PatternConsumerUpdateQueue(PatternMultiTopicsConsumerImpl patternConsumer) {
        this(patternConsumer, patternConsumer.topicsChangeListener);
    }

    /** This constructor is only for test. **/
    @VisibleForTesting
    public PatternConsumerUpdateQueue(PatternMultiTopicsConsumerImpl patternConsumer,
                                      PatternMultiTopicsConsumerImpl.TopicsChangedListener topicsChangeListener) {
        this.patternConsumer = patternConsumer;
        this.topicsChangeListener = topicsChangeListener;
        this.pendingTasks = new LinkedBlockingQueue<>();
    }

    synchronized void appendTopicsAddedOp(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            return;
        }
        doAppend(Pair.of(UpdateSubscriptionType.TOPICS_ADDED, topics));
    }

    synchronized void appendTopicsRemovedOp(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            return;
        }
        doAppend(Pair.of(UpdateSubscriptionType.TOPICS_REMOVED, topics));
    }

    synchronized void appendRecheckOp() {
        doAppend(RECHECK_OP);
    }

    synchronized void doAppend(Pair<UpdateSubscriptionType, Collection<String>> task) {
        // Once there is a recheck task in queue, it means other tasks can be skipped.
        if (recheckTaskInQueue) {
            return;
        }

        // Once there are too many tasks in queue, compress them as a recheck task.
        if (pendingTasks.size() >= 30 && !task.getLeft().equals(UpdateSubscriptionType.RECHECK)) {
            appendRecheckOp();
            return;
        }

        pendingTasks.add(task);
        if (task.getLeft().equals(UpdateSubscriptionType.RECHECK)) {
            recheckTaskInQueue = true;
        }

        // If no task is in-progress, trigger a task execution.
        if (taskInProgress == null) {
            triggerNextTask();
        }
    }

    synchronized void triggerNextTask() {
        if (closed) {
            return;
        }

        Pair<UpdateSubscriptionType, Collection<String>> task = pendingTasks.poll();

        // No pending task.
        if (task == null) {
            taskInProgress = null;
            return;
        }

        // If there is a recheck task in queue, skip others and only call the recheck task.
        if (recheckTaskInQueue && !task.getLeft().equals(UpdateSubscriptionType.RECHECK)) {
            triggerNextTask();
            return;
        }

        // Execute pending task.
        CompletableFuture<Void> newTaskFuture = null;
        switch (task.getLeft()) {
            case TOPICS_ADDED: {
                newTaskFuture = topicsChangeListener.onTopicsAdded(task.getRight());
                break;
            }
            case TOPICS_REMOVED: {
                newTaskFuture = topicsChangeListener.onTopicsRemoved(task.getRight());
                break;
            }
            case RECHECK: {
                recheckTaskInQueue = false;
                lastRecheckTaskStartingTimestamp = System.currentTimeMillis();
                newTaskFuture = patternConsumer.recheckTopicsChange();
            }
        }

        // Trigger next pending task.
        taskInProgress = newTaskFuture;
        newTaskFuture.thenAccept(ignore -> {
           triggerNextTask();
        }).exceptionally(ex -> {
            /**
             * Once a updating fails, trigger a delayed new recheck task to guarantee all things is correct.
             * - Skip if there is already a recheck task in queue.
             * - Skip if the last recheck task has been executed after the current time.
             */
            // TODO log.
            // Skip if there is already a recheck task in queue.
            synchronized (PatternConsumerUpdateQueue.this) {
                if (recheckTaskInQueue || PatternConsumerUpdateQueue.this.closed) {
                    return null;
                }
            }
            // Skip if the last recheck task has been executed after the current time.
            long failedTime = System.currentTimeMillis();
            patternConsumer.client.timer().newTimeout(timeout -> {
                if (lastRecheckTaskStartingTimestamp <= failedTime) {
                    appendRecheckOp();
                }
            }, 30, TimeUnit.SECONDS);
            return null;
        });
    }

    public synchronized CompletableFuture<Void> cancelAllAndWaitForTheRunningTask() {
        this.closed = true;
        return taskInProgress.thenAccept(__ -> {}).exceptionally(ex -> null);
    }

    private enum UpdateSubscriptionType {
        /** Triggered by {@link PatternMultiTopicsConsumerImpl#topicsChangeListener}.**/
        TOPICS_ADDED,
        /** Triggered by {@link PatternMultiTopicsConsumerImpl#topicsChangeListener}.**/
        TOPICS_REMOVED,
        /** A fully check for pattern consumer. **/
        RECHECK;
    }
}

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
package org.apache.pulsar.client.impl;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;

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
 * When you are using this client connect to the broker whose version >= 2.11, there are three scenarios: [1, 2, 4].
 * When you are using this client connect to the broker whose version < 2.11, there is only one scenario: [3] and all
 *   the event will run in the same thread.
 */
@Slf4j
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class PatternConsumerUpdateQueue {
    private final LinkedBlockingQueue<UpdateTask> pendingTasks;

    private final PatternMultiTopicsConsumerImpl<?> patternConsumer;

    private final PatternMultiTopicsConsumerImpl.TopicsChangedListener topicsChangeListener;

    static class UpdateTask {
        private final UpdateSubscriptionType type;

        UpdateTask(UpdateSubscriptionType type) {
            this.type = type;
        }
    }

    static class RecheckTask extends UpdateTask {
        public static final RecheckTask INSTANCE = new RecheckTask();
        private RecheckTask() {
            super(UpdateSubscriptionType.RECHECK);
        }

        @Override
        public String toString() {
            return "RecheckTask";
        }
    }

    static class InitTask extends UpdateTask {
        public static final InitTask INSTANCE = new InitTask();
        private InitTask() {
            super(UpdateSubscriptionType.CONSUMER_INIT);
        }

        @Override
        public String toString() {
            return "InitTask";
        }
    }

    static class TopicsAddedOrRemovedTask extends UpdateTask {
        private final Collection<String> addedTopics;
        private final Collection<String> removedTopics;
        private final String topicsHash;

        public TopicsAddedOrRemovedTask(Collection<String> addedTopics, Collection<String> removedTopics,
                                        String topicsHash) {
            super(UpdateSubscriptionType.TOPICS_CHANGED);
            this.addedTopics = addedTopics;
            this.removedTopics = removedTopics;
            this.topicsHash = topicsHash;
        }

        @Override
        public String toString() {
            return "TopicsAddedOrRemovedTask{" + "addedTopics=" + addedTopics + ", removedTopics=" + removedTopics
                    + ", topicsHash='" + topicsHash + '\'' + '}';
        }
    }

    static class WatchTopicListSuccessTask extends UpdateTask {
        private final CommandWatchTopicListSuccess response;
        private final String localStateTopicsHash;
        private final int epoch;

        WatchTopicListSuccessTask(CommandWatchTopicListSuccess response, String localStateTopicsHash, int epoch) {
            super(UpdateSubscriptionType.WATCH_TOPIC_LIST_SUCCESS);
            this.response = response;
            this.localStateTopicsHash = localStateTopicsHash;
            this.epoch = epoch;
        }
    }

    /**
     * Whether there is a task is in progress, this variable is used to confirm whether a next-task triggering is
     * needed.
     */
    private Pair<UpdateSubscriptionType, CompletableFuture<Void>> taskInProgress = null;

    /**
     * Whether there is a recheck task in queue.
     * - Since recheck task will do all changes, it can be used to compress multiple tasks to one.
     * - To avoid skipping the newest changes, once the recheck task is starting to work, this variable will be set
     *   to "false".
     */
    private boolean recheckTaskInQueue = false;

    private volatile long lastRecheckTaskStartingTimestamp = 0;

    private boolean closed;

    public PatternConsumerUpdateQueue(PatternMultiTopicsConsumerImpl<?> patternConsumer) {
        this(patternConsumer, patternConsumer.topicsChangeListener);
    }

    /** This constructor is only for test. **/
    @VisibleForTesting
    @SuppressWarnings("this-escape")
    public PatternConsumerUpdateQueue(PatternMultiTopicsConsumerImpl<?> patternConsumer,
                                      PatternMultiTopicsConsumerImpl.TopicsChangedListener topicsChangeListener) {
        this.patternConsumer = patternConsumer;
        this.topicsChangeListener = topicsChangeListener;
        this.pendingTasks = new LinkedBlockingQueue<>();
        // To avoid subscribing and topics changed events execute concurrently, let the change events starts after the
        // subscribing task.
        doAppend(InitTask.INSTANCE);
    }

    synchronized void appendTopicsChangedOp(Collection<String> addedTopics, Collection<String> deletedTopics,
                                            String topicsHash) {
        doAppend(new TopicsAddedOrRemovedTask(addedTopics, deletedTopics, topicsHash));
    }

    synchronized void appendRecheckOp() {
        doAppend(RecheckTask.INSTANCE);
    }

    synchronized void appendWatchTopicListSuccessOp(CommandWatchTopicListSuccess response, String localStateTopicsHash,
                                              int epoch) {
        doAppend(new WatchTopicListSuccessTask(response, localStateTopicsHash, epoch));
    }

    synchronized void doAppend(UpdateTask task) {
        if (log.isDebugEnabled()) {
            log.debug("Pattern consumer [{}] try to append task. {}", patternConsumer.getSubscription(), task);
        }
        // Once there is a recheck task in queue, it means other tasks can be skipped.
        if (recheckTaskInQueue) {
            return;
        }

        // Once there are too many tasks in queue, compress them as a recheck task.
        if (pendingTasks.size() >= 30 && task.type != UpdateSubscriptionType.RECHECK) {
            appendRecheckOp();
            return;
        }

        pendingTasks.add(task);
        if (task.type == UpdateSubscriptionType.RECHECK) {
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

        final UpdateTask task = pendingTasks.poll();

        // No pending task.
        if (task == null) {
            taskInProgress = null;
            return;
        }

        // If there is a recheck task in queue, skip others and only call the recheck task.
        if (recheckTaskInQueue && task.type != UpdateSubscriptionType.RECHECK) {
            triggerNextTask();
            return;
        }

        // Execute pending task.
        CompletableFuture<Void> newTaskFuture = null;
        switch (task.type) {
            case CONSUMER_INIT: {
                newTaskFuture = patternConsumer.getSubscribeFuture().thenAccept(__ -> {}).exceptionally(ex -> {
                    // If the subscribe future was failed, the consumer will be closed.
                    synchronized (PatternConsumerUpdateQueue.this) {
                        this.closed = true;
                        patternConsumer.closeAsync().exceptionally(ex2 -> {
                            log.error("Pattern consumer failed to close, this error may left orphan consumers."
                                    + " Subscription: {}", patternConsumer.getSubscription());
                            return null;
                        });
                    }
                    return null;
                });
                break;
            }
            case TOPICS_CHANGED: {
                TopicsAddedOrRemovedTask topicsAddedOrRemovedTask = (TopicsAddedOrRemovedTask) task;
                newTaskFuture = topicsChangeListener.onTopicsRemoved(topicsAddedOrRemovedTask.removedTopics)
                        .thenCompose(__ ->
                                topicsChangeListener.onTopicsAdded(topicsAddedOrRemovedTask.addedTopics))
                        .thenRun(() -> {
                            if (!patternConsumer.supportsTopicListWatcherReconcile()) {
                                // Ignore the topics hash until topic-list watcher reconciliation is supported.
                                // Broker-side state can be stale, which would trigger unnecessary reconciliation.
                                // The client will reconcile later when it fetches the topic list after the next
                                // patternAutoDiscoveryPeriod interval.
                                // Brokers that support watcher reconciliation also refresh broker-side state
                                // when reconciliation is requested.
                                // Older brokers have known topic-listing bugs (issue 25192: system topics included),
                                // so their hash is not reliable anyway.
                                return;
                            }
                            String localHash = patternConsumer.getLocalStateTopicsHash();
                            String brokerHash = topicsAddedOrRemovedTask.topicsHash;
                            if (brokerHash != null && brokerHash.length() > 0 && !brokerHash.equals(localHash)) {
                                log.info("[{}][{}] Hash mismatch detected (local: {}, broker: {}). Triggering "
                                                + "reconciliation.", patternConsumer.getPattern().inputPattern(),
                                        patternConsumer.getSubscription(), localHash, brokerHash);
                                appendRecheckOp();
                            }
                        });
                break;
            }
            case RECHECK: {
                recheckTaskInQueue = false;
                lastRecheckTaskStartingTimestamp = System.currentTimeMillis();
                newTaskFuture = patternConsumer.recheckTopicsChange();
                break;
            }
            case WATCH_TOPIC_LIST_SUCCESS: {
                WatchTopicListSuccessTask watchTopicListSuccessTask = (WatchTopicListSuccessTask) task;
                newTaskFuture = patternConsumer.handleWatchTopicListSuccess(watchTopicListSuccessTask.response,
                        watchTopicListSuccessTask.localStateTopicsHash, watchTopicListSuccessTask.epoch);
                break;
            }
            default: {
                throw new RuntimeException("Un-support UpdateSubscriptionType");
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Pattern consumer [{}] starting task. {}", patternConsumer.getSubscription(), task);
        }
        // Trigger next pending task.
        taskInProgress = Pair.of(task.type, newTaskFuture);
        newTaskFuture.thenAccept(ignore -> {
            if (log.isDebugEnabled()) {
                log.debug("Pattern consumer [{}] task finished. {}", patternConsumer.getSubscription(), task);
            }
            triggerNextTask();
        }).exceptionally(ex -> {
            /**
             * Once a updating fails, trigger a delayed new recheck task to guarantee all things is correct.
             * - Skip if there is already a recheck task in queue.
             * - Skip if the last recheck task has been executed after the current time.
             */
            log.error("Pattern consumer [{}] task finished. {}. But it failed", patternConsumer.getSubscription(),
                    task, ex);
            // Skip if there is already a recheck task in queue.
            synchronized (PatternConsumerUpdateQueue.this) {
                if (recheckTaskInQueue || PatternConsumerUpdateQueue.this.closed) {
                    return null;
                }
            }
            // Skip if the last recheck task has been executed after the current time.
            long failedTime = System.currentTimeMillis();
            patternConsumer.getClient().timer().newTimeout(timeout -> {
                if (lastRecheckTaskStartingTimestamp <= failedTime) {
                    appendRecheckOp();
                }
            }, 10, TimeUnit.SECONDS);
            triggerNextTask();
            return null;
        });
    }

    public synchronized CompletableFuture<Void> cancelAllAndWaitForTheRunningTask() {
        this.closed = true;
        if (taskInProgress == null) {
            return CompletableFuture.completedFuture(null);
        }
        // If the in-progress task is consumer init task, it means nothing is in-progress.
        if (taskInProgress.getLeft().equals(UpdateSubscriptionType.CONSUMER_INIT)) {
            return CompletableFuture.completedFuture(null);
        }
        return taskInProgress.getRight().thenAccept(__ -> {}).exceptionally(ex -> null);
    }

    private enum UpdateSubscriptionType {
        /** A marker that indicates the consumer's subscribe task. **/
        CONSUMER_INIT,
        /** Triggered by topic list watcher when topics changed. **/
        TOPICS_CHANGED,
        /** A fully check for pattern consumer. **/
        RECHECK,
        /** Handle initial watch topic list success response. **/
        WATCH_TOPIC_LIST_SUCCESS;
    }
}

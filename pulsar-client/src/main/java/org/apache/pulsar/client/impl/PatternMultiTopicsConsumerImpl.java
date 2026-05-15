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
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.github.merlimat.slog.Logger;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.util.FutureUtil;

public class PatternMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> implements TimerTask {
    private static final Logger LOG = Logger.get(PatternMultiTopicsConsumerImpl.class);
    private final Logger log;
    private final TopicsPattern topicsPattern;
    final TopicsChangedListener topicsChangeListener;
    private final Mode subscriptionMode;
    @Getter(value = AccessLevel.PROTECTED, onMethod_ = @VisibleForTesting)
    private volatile TopicListWatcher topicListWatcher;
    @Getter(onMethod_ = @VisibleForTesting)
    private final CompletableFuture<TopicListWatcher> watcherFuture = new CompletableFuture<>();
    protected NamespaceName namespaceName;

    private final AtomicInteger recheckPatternEpoch = new AtomicInteger();
    private volatile Timeout recheckPatternTimeout = null;

    private PatternConsumerUpdateQueue updateTaskQueue;
    private volatile boolean closed = false;

    /***
     * @param topicsPattern The regexp for the topic name(not contains partition suffix).
     */
    public PatternMultiTopicsConsumerImpl(TopicsPattern topicsPattern,
                                          PulsarClientImpl client,
                                          ConsumerConfigurationData<T> conf,
                                          ExecutorProvider executorProvider,
                                          CompletableFuture<Consumer<T>> subscribeFuture,
                                          Schema<T> schema,
                                          Mode subscriptionMode,
                                          ConsumerInterceptors<T> interceptors) {
        super(client, conf, executorProvider, subscribeFuture, schema, interceptors,
                false /* createTopicIfDoesNotExist */);
        this.log = LOG.with()
                .attr("subscription", conf.getSubscriptionName())
                .build();
        this.topicsPattern = topicsPattern;
        this.subscriptionMode = subscriptionMode;
        this.namespaceName = topicsPattern.namespace();
        this.topicsChangeListener = new PatternTopicsChangedListener();
        this.updateTaskQueue = new PatternConsumerUpdateQueue(this);
        if (subscriptionMode == Mode.PERSISTENT) {
            subscribeFuture.whenComplete((__, exception) -> {
                if (!closed && exception == null) {
                    long watcherId = client.newTopicListWatcherId();
                    topicListWatcher = new TopicListWatcher(updateTaskQueue, client, topicsPattern, watcherId,
                            namespaceName, this::getLocalStateTopicsHash, watcherFuture,
                            this::getNextRecheckPatternEpoch);
                    watcherFuture.whenComplete((watcher, ex) -> {
                        if (closed) {
                            log.warn().exception(ex)
                                    .log("Pattern consumer was closed while creating topic list watcher");
                        } else if (ex != null) {
                            if (ex instanceof PulsarClientException.NotAllowedException) {
                                // create info message when topic watchers aren't supported
                                log.info().exceptionMessage(ex)
                                        .log("Pattern consumer unable to create topic list watcher");
                            } else {
                                log.warn().exception(ex)
                                        .log("Pattern consumer unable to create topic list watcher");
                            }
                        }
                        scheduleRecheckTopics();
                    });
                }
            });
        } else {
            log.debug().attr("mode", subscriptionMode)
                    .log("Pattern consumer not creating topic list watcher for subscription mode");
            topicListWatcher = null;
            watcherFuture.complete(null);
            subscribeFuture.whenComplete((__, ex) -> {
                if (!closed && ex == null) {
                    scheduleRecheckTopics();
                }
            });
        }
    }

    /**
     * This method will be called after the {@link TopicListWatcher} reconnected after enabled {@link TopicListWatcher}.
     */
    private void recheckTopicsChangeAfterReconnect() {
        // Skip if closed or the task has been cancelled.
        if (getState() == State.Closing || getState() == State.Closed) {
            return;
        }
        // Do check.
        updateTaskQueue.appendRecheckOp();
    }

    // TimerTask to recheck topics change, and trigger subscribe/unsubscribe based on the change.
    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled() || closed) {
            return;
        }
        updateTaskQueue.appendRecheckOp();
    }

    CompletableFuture<Void> recheckTopicsChange() {
        final int epoch = getNextRecheckPatternEpoch();

        CompletableFuture<Void> recheckFuture;
        // Prefer watcher-based reconcile when a watcher exists and is connected. Fallback to lookup if watcher
        // is not available or the watcher-based request fails.
        if (supportsTopicListWatcherReconcile()) {
            String localStateTopicsHash = getLocalStateTopicsHash();
            recheckFuture = topicListWatcher.reconcile(localStateTopicsHash).thenCompose(response -> {
                return handleWatchTopicListSuccess(response, localStateTopicsHash, epoch);
            }).handle((res, ex) -> {
                if (ex != null) {
                    // watcher-based reconcile failed -> fall back to lookup-based recheck
                    return doLookupBasedRecheck(epoch);
                } else {
                    // watcher-based reconcile completed successfully
                    return CompletableFuture.<Void>completedFuture(null);
                }
            }).thenCompose(Function.identity());
        } else {
            // Fallback: perform the existing lookup-based recheck
            recheckFuture = doLookupBasedRecheck(epoch);
        }

        return recheckFuture.handle((__, ex) -> {
            if (ex != null) {
                log.info().attr("inputPattern", getPattern().inputPattern())
                        .exceptionMessage(ex)
                        .log("Pattern consumer failed to recheck topics changes");
            }
            scheduleRecheckTopics();
            return null;
        });
    }

    int getNextRecheckPatternEpoch() {
        return recheckPatternEpoch.incrementAndGet();
    }

    CompletableFuture<Void> handleWatchTopicListSuccess(CommandWatchTopicListSuccess response,
                                                        String localStateTopicsHash, int epoch) {
        synchronized (PatternMultiTopicsConsumerImpl.this) {
            if (recheckPatternEpoch.get() > epoch) {
                return CompletableFuture.completedFuture(null);
            }
            // Build a GetTopicsResult-like object from the watch response
            // so we can reuse updateSubscriptions
            final List<String> topics = (response != null)
                    ? response.getTopicsList()
                    : Collections.emptyList();
            final String hash = (response != null && response.hasTopicsHash())
                    ? response.getTopicsHash()
                    : null;
            final boolean changed = !localStateTopicsHash.equals(hash);
            final GetTopicsResult getTopicsResult =
                    new GetTopicsResult(topics, hash, true, changed);

            final List<String> oldTopics = new ArrayList<>(getPartitions());
            return updateSubscriptions(topicsPattern, getTopicsResult, topicsChangeListener, oldTopics,
                    subscription);
        }
    }

    boolean supportsTopicListWatcherReconcile() {
        return topicListWatcher != null && topicListWatcher.supportsReconcile() && watcherFuture.isDone()
                && !watcherFuture.isCompletedExceptionally() && topicListWatcher.isConnected();
    }

    private synchronized void scheduleRecheckTopics() {
        if (!closed) {
            // cancel previous timeout if it exists
            Timeout oldTimeout = this.recheckPatternTimeout;
            if (oldTimeout != null) {
                // cancel is a no-op if the timeout has already been executed or cancelled
                oldTimeout.cancel();
            }
            this.recheckPatternTimeout = client.timer().newTimeout(this,
                    Math.max(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.SECONDS);
        }
    }

    private CompletableFuture<Void> doLookupBasedRecheck(final int epoch) {
        final String pattern = topicsPattern.inputPattern();
        return client.getLookup()
                .getTopicsUnderNamespace(namespaceName, subscriptionMode, pattern,
                    getLocalStateTopicsHash(), conf.getProperties())
                .thenCompose(getTopicsResult -> {
                    // If "recheckTopicsChange" has been called more than one times, only make the last one take
                    // affects.
                    // Use "synchronized (recheckPatternTaskBackoff)" instead of
                    // `synchronized(PatternMultiTopicsConsumerImpl.this)` to avoid locking in a wider range.
                    synchronized (PatternMultiTopicsConsumerImpl.this) {
                        if (recheckPatternEpoch.get() > epoch) {
                            return CompletableFuture.completedFuture(null);
                        }
                        log.debug().attr("namespace", namespaceName)
                                .attr("topicsSize", getTopicsResult.getTopics().size())
                                .attr("topicsHash", getTopicsResult.getTopicsHash())
                                .attr("filtered", getTopicsResult.isFiltered())
                                .log("Pattern consumer get topics under namespace");
                        getTopicsResult.getTopics().forEach(topicName ->
                                log.debug().attr("namespace", namespaceName)
                                        .attr("topic", topicName)
                                        .log("Get topics under namespace"));

                        final List<String> oldTopics = new ArrayList<>(getPartitions());
                        return updateSubscriptions(topicsPattern, getTopicsResult, topicsChangeListener, oldTopics,
                                subscription);
                    }
                });
    }

    static CompletableFuture<Void> updateSubscriptions(TopicsPattern topicsPattern,
                                                       GetTopicsResult getTopicsResult,
                                                       TopicsChangedListener topicsChangedListener,
                                                       List<String> oldTopics,
                                                       String subscriptionForLog) {
        if (!getTopicsResult.isChanged()) {
            return CompletableFuture.completedFuture(null);
        }

        List<String> newTopics;
        if (getTopicsResult.isFiltered()) {
            newTopics = getTopicsResult.getNonPartitionedOrPartitionTopics();
        } else {
            newTopics = getTopicsResult.filterTopics(topicsPattern).getNonPartitionedOrPartitionTopics();
        }

        final List<CompletableFuture<?>> listenersCallback = new ArrayList<>(2);
        Set<String> topicsAdded = TopicList.minus(newTopics, oldTopics);
        Set<String> topicsRemoved = TopicList.minus(oldTopics, newTopics);
        LOG.debug().attr("subscription", subscriptionForLog)
                .attr("topicsAdded", topicsAdded)
                .attr("topicsRemoved", topicsRemoved)
                .log("Pattern consumer rechecking topics");
        listenersCallback.add(topicsChangedListener.onTopicsAdded(topicsAdded));
        listenersCallback.add(topicsChangedListener.onTopicsRemoved(topicsRemoved));
        return FutureUtil.waitForAll(Collections.unmodifiableList(listenersCallback));
    }

    public TopicsPattern getPattern() {
        return this.topicsPattern;
    }

    interface TopicsChangedListener {
        /***
         * unsubscribe and delete {@link ConsumerImpl} in the {@link MultiTopicsConsumerImpl#consumers} map in
         * {@link MultiTopicsConsumerImpl}.
         * @param removedTopics topic names removed(contains the partition suffix).
         */
        CompletableFuture<Void> onTopicsRemoved(Collection<String> removedTopics);

        /***
         * subscribe and create a list of new {@link ConsumerImpl}, added them to the
         * {@link MultiTopicsConsumerImpl#consumers} map in {@link MultiTopicsConsumerImpl}.
         * @param addedTopics topic names added(contains the partition suffix).
         */
        CompletableFuture<Void> onTopicsAdded(Collection<String> addedTopics);
    }

    private class PatternTopicsChangedListener implements TopicsChangedListener {

        /**
         * {@inheritDoc}
         */
        @Override
        public CompletableFuture<Void> onTopicsRemoved(Collection<String> removedTopics) {
            if (removedTopics.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }

            // Unsubscribe and remove consumers in memory.
            List<CompletableFuture<Void>> unsubscribeList = new ArrayList<>(removedTopics.size());
            Set<String> partialRemoved = new HashSet<>(removedTopics.size());
            Set<String> partialRemovedForLog = new HashSet<>(removedTopics.size());
            for (String tp : removedTopics) {
                TopicName topicName = TopicName.get(tp);
                ConsumerImpl<T> consumer = consumers.get(topicName.toString());
                if (consumer != null) {
                    CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
                    consumer.closeAsync().whenComplete((__, ex) -> {
                        if (ex != null) {
                            log.error().attr("topic", topicName.toString())
                                    .exception(ex)
                                    .log("Pattern consumer failed to unsubscribe from topic");
                            unsubscribeFuture.completeExceptionally(ex);
                        } else {
                            consumers.remove(topicName.toString(), consumer);
                            unsubscribeFuture.complete(null);
                        }
                    });
                    unsubscribeList.add(unsubscribeFuture);
                    partialRemoved.add(topicName.getPartitionedTopicName());
                    partialRemovedForLog.add(topicName.toString());
                }
            }
            log.debug().attr("topics", partialRemovedForLog)
                    .log("Pattern consumer remove topics");

            // Remove partitioned topics in memory.
            return FutureUtil.waitForAll(unsubscribeList).handle((__, ex) -> {
                List<String> removedPartitionedTopicsForLog = new ArrayList<>();
                for (String groupedTopicRemoved : partialRemoved) {
                    Integer partitions = partitionedTopics.get(groupedTopicRemoved);
                    if (partitions != null) {
                        boolean allPartitionsHasBeenRemoved = true;
                        for (int i = 0; i < partitions; i++) {
                            if (consumers.containsKey(
                                    TopicName.get(groupedTopicRemoved).getPartition(i).toString())) {
                                allPartitionsHasBeenRemoved = false;
                                break;
                            }
                        }
                        if (allPartitionsHasBeenRemoved) {
                            removedPartitionedTopicsForLog.add(String.format("%s with %s partitions",
                                    groupedTopicRemoved, partitions));
                            partitionedTopics.remove(groupedTopicRemoved, partitions);
                        }
                    }
                }
                log.debug().attr("removed", removedPartitionedTopicsForLog)
                        .log("Pattern consumer removed partitioned topics because all partitions have been removed");
                return null;
            });
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CompletableFuture<Void> onTopicsAdded(Collection<String> addedTopics) {
            if (addedTopics.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(addedTopics.size());
            /**
             * Three normal cases:
             *  1. Expand partitions.
             *  2. Non-partitioned topic, but has been subscribing.
             *  3. Non-partitioned topic or Partitioned topic, but has not been subscribing.
             * Two unexpected cases:
             *   Error-1: Received adding non-partitioned topic event, but has subscribed a partitioned topic with the
             *     same name.
             *   Error-2: Received adding partitioned topic event, but has subscribed a non-partitioned topic with the
             *     same name.
             *
             * Note: The events that triggered by {@link TopicsPartitionChangedListener} after expanding partitions has
             *    been disabled through "conf.setAutoUpdatePartitions(false)" when creating
             *    {@link PatternMultiTopicsConsumerImpl}.
             */
            Set<String> groupedTopics = new HashSet<>();
            List<String> expendPartitionsForLog = new ArrayList<>();
            for (String tp : addedTopics) {
                TopicName topicName = TopicName.get(tp);
                groupedTopics.add(topicName.getPartitionedTopicName());
            }
            for (String tp : addedTopics) {
                TopicName topicName = TopicName.get(tp);
                // Case 1: Expand partitions.
                if (partitionedTopics.containsKey(topicName.getPartitionedTopicName())) {
                    if (consumers.containsKey(topicName.toString())) {
                        // Already subscribed.
                    } else if (topicName.getPartitionIndex() < 0) {
                        // Error-1: Received adding non-partitioned topic event, but has subscribed a partitioned topic
                        // with the same name.
                        log.error().attr("topic", topicName.toString())
                                .log("Pattern consumer skip subscribing to"
                                        + " non-partitioned topic because a"
                                        + " partitioned topic with the same"
                                        + " name already exists");
                    } else {
                        if (topicName.getPartitionIndex() + 1
                                > partitionedTopics.get(topicName.getPartitionedTopicName())) {
                            partitionedTopics.put(topicName.getPartitionedTopicName(),
                                    topicName.getPartitionIndex() + 1);
                        }
                        expendPartitionsForLog.add(topicName.toString());
                        CompletableFuture<Void> consumerFuture = subscribeAsync(topicName.toString(),
                                PartitionedTopicMetadata.NON_PARTITIONED);
                        consumerFuture.whenComplete((__, ex) -> {
                            if (ex != null) {
                                log.warn().attr("topic", topicName)
                                        .exception(ex)
                                        .log("Pattern consumer failed to subscribe to topic");
                            }
                        });
                        futures.add(consumerFuture);
                    }
                    groupedTopics.remove(topicName.getPartitionedTopicName());
                } else if (consumers.containsKey(topicName.toString())) {
                    // Case-2: Non-partitioned topic, but has been subscribing.
                    groupedTopics.remove(topicName.getPartitionedTopicName());
                } else if (consumers.containsKey(topicName.getPartitionedTopicName())
                        && topicName.getPartitionIndex() >= 0) {
                    // Error-2: Received adding partitioned topic event, but has subscribed a non-partitioned topic
                    // with the same name.
                    log.error().attr("topic", topicName)
                            .log("Pattern consumer skip subscribing to"
                                    + " partitioned topic because a"
                                    + " non-partitioned topic with the same"
                                    + " name already exists");
                    groupedTopics.remove(topicName.getPartitionedTopicName());
                }
            }
            // Case 3: Non-partitioned topic or Partitioned topic, which has not been subscribed.
            for (String partitionedTopic : groupedTopics) {
                CompletableFuture<Void> consumerFuture = subscribeAsync(partitionedTopic, false);
                consumerFuture.whenComplete((__, ex) -> {
                    if (ex != null) {
                        log.warn().attr("topic", partitionedTopic)
                                .exception(ex)
                                .log("Pattern consumer failed to subscribe to topic");
                    }
                });
                futures.add(consumerFuture);
            }
            log.debug().attr("partitions", expendPartitionsForLog)
                    .attr("subscribing", groupedTopics)
                    .log("Pattern consumer add topics");
            return FutureUtil.waitForAll(futures);
        }
    }

    @Override
    @SuppressFBWarnings
    public CompletableFuture<Void> closeAsync() {
        closed = true;
        Timeout timeout = recheckPatternTimeout;
        if (timeout != null) {
            timeout.cancel();
            recheckPatternTimeout = null;
        }
        CompletableFuture<Void> topicListWatcherCloseFuture =
                Optional.ofNullable(topicListWatcher).map(TopicListWatcher::closeAsync)
                        .orElse(CompletableFuture.completedFuture(null)).exceptionally(t -> null);
        CompletableFuture<Void> runningTaskCancelFuture = updateTaskQueue.cancelAllAndWaitForTheRunningTask();
        return FutureUtil.waitForAll(Lists.newArrayList(topicListWatcherCloseFuture, runningTaskCancelFuture))
                .exceptionally(t -> null).thenCompose(__ -> super.closeAsync());
    }

    @VisibleForTesting
    int getRecheckPatternEpoch() {
        return recheckPatternEpoch.get();
    }

    @VisibleForTesting
    Timeout getRecheckPatternTimeout() {
        return recheckPatternTimeout;
    }

    /**
     * Get the current topics hash calculated from the pattern consumer's topic list.
     * This is used to validate incremental updates against the broker's hash.
     */
    @VisibleForTesting
    String getLocalStateTopicsHash() {
        return TopicList.calculateHash(getPartitions());
    }

    protected void handleSubscribeOneTopicError(String topicName,
                                                Throwable error,
                                                CompletableFuture<Void> subscribeFuture) {
        subscribeFuture.completeExceptionally(error);
    }
}

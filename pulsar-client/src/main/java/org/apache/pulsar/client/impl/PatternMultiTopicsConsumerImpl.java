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

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.re2j.Pattern;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.BackoffBuilder;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatternMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> implements TimerTask {
    private final Pattern topicsPattern;
    private final TopicsChangedListener topicsChangeListener;
    private final Mode subscriptionMode;
    private final CompletableFuture<TopicListWatcher> watcherFuture = new CompletableFuture<>();
    protected NamespaceName namespaceName;

    /**
     * There is two task to re-check topic changes, the both tasks will not be take affects at the same time.
     * 1. {@link #recheckTopicsChangeAfterReconnect}: it will be called after the {@link TopicListWatcher} reconnected
     *     if you enabled {@link TopicListWatcher}. This backoff used to do a retry if
     *     {@link #recheckTopicsChangeAfterReconnect} is failed.
     * 2. {@link #run} A scheduled task to trigger re-check topic changes, it will be used if you disabled
     *     {@link TopicListWatcher}.
     */
    private final Backoff recheckPatternTaskBackoff;
    private final AtomicInteger recheckPatternEpoch = new AtomicInteger();
    private volatile Timeout recheckPatternTimeout = null;
    private volatile String topicsHash;

    /**
     * This variable is used to make all tasks that will modify subscriptions will be executed one by one.
     *
     * So far, four three scenarios that will modify subscriptions:
     * 1. When start pattern consumer.
     * 2. After topic list watcher reconnected, it will call {@link #recheckTopicsChange()}. this scenario only
     *    exists in the version >= 2.11.
     * 3. A scheduled task {@link #recheckPatternTimeout} will call {@link #recheckTopicsChange()}, this scenario only
     *    exists in the version < 2.11.
     * 4. The topics change events will trigger a {@link #recheckTopicsChange()}.
     *
     * So when you are using a release >= 2.11, there are three scenarios: [1, 2, 4].
     * - The events related scenario-4 will be executed at the same thread, because it will be triggered by
     *   {@link ClientCnx};
     * - The events related scenario-2 will be executed after switching {@link ClientCnx}, so this event will happen
     *   after the events related to scenario-4, this guarantees the variable {@link #updateFuture} will be
     *   updated before the new {@link ClientCnx} was set.
     * - If the update subscription task related scenario-2 fails, it will schedule a retry task. Since it checks all
     *   topics, so all things will be fine if it is later than any events related scenario-4.
     *
     * When you are using a release < 2.11, there are three scenarios: [1, 3, 4]. Since it checks all topics,
     * so all things will be fine. TODO compare and set.
     */
    private volatile CompletableFuture<Void> updateFuture;

    /***
     * @param topicsPattern The regexp for the topic name(not contains partition suffix).
     */
    public PatternMultiTopicsConsumerImpl(Pattern topicsPattern,
                                          String topicsHash,
                                          PulsarClientImpl client,
                                          ConsumerConfigurationData<T> conf,
                                          ExecutorProvider executorProvider,
                                          CompletableFuture<Consumer<T>> subscribeFuture,
                                          Schema<T> schema,
                                          Mode subscriptionMode,
                                          ConsumerInterceptors<T> interceptors) {
        super(client, conf, executorProvider, subscribeFuture, schema, interceptors,
                false /* createTopicIfDoesNotExist */);
        updateFuture = subscribeFuture.thenAccept(__ -> {}).exceptionally(ex -> null);
        this.topicsPattern = topicsPattern;
        this.topicsHash = topicsHash;
        this.subscriptionMode = subscriptionMode;
        this.recheckPatternTaskBackoff = new BackoffBuilder()
                .setInitialTime(client.getConfiguration().getInitialBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMax(client.getConfiguration().getMaxBackoffIntervalNanos(), TimeUnit.NANOSECONDS)
                .setMandatoryStop(0, TimeUnit.SECONDS)
                .create();

        if (this.namespaceName == null) {
            this.namespaceName = getNameSpaceFromPattern(topicsPattern);
        }
        checkArgument(getNameSpaceFromPattern(topicsPattern).toString().equals(this.namespaceName.toString()));

        this.topicsChangeListener = new PatternTopicsChangedListener();
        this.recheckPatternTimeout = client.timer()
                .newTimeout(this, Math.max(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.SECONDS);
        if (subscriptionMode == Mode.PERSISTENT) {
            long watcherId = client.newTopicListWatcherId();
            new TopicListWatcher(topicsChangeListener, client, topicsPattern, watcherId,
                namespaceName, topicsHash, watcherFuture, () -> recheckTopicsChangeAfterReconnect());
            watcherFuture
               .thenAccept(__ -> recheckPatternTimeout.cancel())
               .exceptionally(ex -> {
                   log.warn("Unable to create topic list watcher. Falling back to only polling for new topics", ex);
                   return null;
               });
        } else {
            log.debug("Not creating topic list watcher for subscription mode {}", subscriptionMode);
            watcherFuture.complete(null);
        }
    }

    public static NamespaceName getNameSpaceFromPattern(Pattern pattern) {
        return TopicName.get(pattern.pattern()).getNamespaceObject();
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
        recheckTopicsChange().whenComplete((ignore, ex) -> {
            if (ex != null) {
                log.warn("[{}] Failed to recheck topics change: {}", topic, ex.getMessage());
                long delayMs = recheckPatternTaskBackoff.next();
                client.timer().newTimeout(timeout -> {
                    recheckTopicsChangeAfterReconnect();
                }, delayMs, TimeUnit.MILLISECONDS);
            } else {
                recheckPatternTaskBackoff.reset();
            }
        });
    }

    // TimerTask to recheck topics change, and trigger subscribe/unsubscribe based on the change.
    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }
        recheckTopicsChange().exceptionally(ex -> {
            log.warn("[{}] Failed to recheck topics change: {}", topic, ex.getMessage());
            return null;
        }).thenAccept(__ -> {
            // schedule the next re-check task
            this.recheckPatternTimeout = client.timer()
                    .newTimeout(PatternMultiTopicsConsumerImpl.this,
                    Math.max(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.SECONDS);
        });
    }

    private CompletableFuture<Void> recheckTopicsChange() {
        String pattern = topicsPattern.pattern();
        final int epoch = recheckPatternEpoch.incrementAndGet();
        return client.getLookup().getTopicsUnderNamespace(namespaceName, subscriptionMode, pattern, topicsHash)
            .thenCompose(getTopicsResult -> {
                // If "recheckTopicsChange" has been called more than one times, only make the last one take affects.
                // Use "synchronized (recheckPatternTaskBackoff)" instead of
                // `synchronized(PatternMultiTopicsConsumerImpl.this)` to avoid locking in a wider range.
                synchronized (recheckPatternTaskBackoff) {
                    if (recheckPatternEpoch.get() > epoch) {
                        return CompletableFuture.completedFuture(null);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Get topics under namespace {}, topics.size: {}, topicsHash: {}, filtered: {}",
                                namespaceName, getTopicsResult.getTopics().size(), getTopicsResult.getTopicsHash(),
                                getTopicsResult.isFiltered());
                        getTopicsResult.getTopics().forEach(topicName ->
                                log.debug("Get topics under namespace {}, topic: {}", namespaceName, topicName));
                    }

                    final List<String> oldTopics = new ArrayList<>(getPartitions());
                    return updateSubscriptions(topicsPattern, this::setTopicsHash, getTopicsResult,
                            topicsChangeListener, oldTopics);
                }
            });
    }

    static CompletableFuture<Void> updateSubscriptions(Pattern topicsPattern,
                                                       java.util.function.Consumer<String> topicsHashSetter,
                                                       GetTopicsResult getTopicsResult,
                                                       TopicsChangedListener topicsChangedListener,
                                                       List<String> oldTopics) {
        topicsHashSetter.accept(getTopicsResult.getTopicsHash());
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
        listenersCallback.add(topicsChangedListener.onTopicsAdded(TopicList.minus(newTopics, oldTopics)));
        listenersCallback.add(topicsChangedListener.onTopicsRemoved(TopicList.minus(oldTopics, newTopics)));
        return FutureUtil.waitForAll(Collections.unmodifiableList(listenersCallback));
    }

    public Pattern getPattern() {
        return this.topicsPattern;
    }

    @VisibleForTesting
    void setTopicsHash(String topicsHash) {
        this.topicsHash = topicsHash;
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

            CompletableFuture<Void> newFuture = updateFuture.thenCompose(ignore -> {
                // Unsubscribe and remove consumers in memory.
                List<CompletableFuture<Void>> unsubscribeList = new ArrayList<>(removedTopics.size());
                Set<String> partialRemoved = new HashSet<>(removedTopics.size());
                for (String tp : removedTopics) {
                    ConsumerImpl<T> consumer = consumers.get(tp);
                    if (consumer != null) {
                        CompletableFuture<Void> unsubscribeFuture = new CompletableFuture<>();
                        consumer.closeAsync().whenComplete((__, ex) -> {
                            if (ex != null) {
                                log.error("[{}] Failed to unsubscribe from topics: {}", tp, ex);
                                unsubscribeFuture.completeExceptionally(ex);
                            } else {
                                consumers.remove(tp, consumer);
                                unsubscribeFuture.complete(null);
                            }
                        });
                        unsubscribeList.add(unsubscribeFuture);
                        partialRemoved.add(TopicName.get(tp).getPartitionedTopicName());
                    }
                }

                // Remove partitioned topics in memory.
                return FutureUtil.waitForAll(unsubscribeList).whenComplete((__, ex) -> {
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
                                partitionedTopics.remove(groupedTopicRemoved, partitions);
                            }
                        }
                    }
                });
            });
            return updateFuture = newFuture.exceptionally(ex -> null);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public CompletableFuture<Void> onTopicsAdded(Collection<String> addedTopics) {
            if (addedTopics.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> newFuture = updateFuture.thenCompose(ignore -> {
                List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(addedTopics.size());
                /**
                 * Three cases:
                 *  1. Expand partitions.
                 *  2. Non-partitioned topic, but has been subscribing.
                 *  3. Non-partitioned topic or Partitioned topic, but has not been subscribing.
                 */
                Set<String> groupedTopics = new HashSet<>();
                for (String tp : addedTopics) {
                    TopicName topicName = TopicName.get(tp);
                    groupedTopics.add(topicName.getPartitionedTopicName());
                }
                for (String tp : addedTopics) {
                    TopicName topicName = TopicName.get(tp);
                    // Case 1: Expand partitions.
                    if (partitionedTopics.containsKey(topicName.getPartitionedTopicName())) {
                        if (consumers.containsKey(tp.toString())) {
                            continue;
                        } else {
                            if (topicName.getPartitionIndex() + 1 >
                                    partitionedTopics.get(topicName.getPartitionedTopicName())) {
                                partitionedTopics.put(topicName.getPartitionedTopicName(),
                                        topicName.getPartitionIndex() + 1);
                            }
                            CompletableFuture consumerFuture = subscribeAsync(tp, 0);
                            consumerFuture.whenComplete((__, ex) -> {
                                if (ex != null) {
                                    log.warn("[{}] Failed to subscribe to topics: {}", tp, ex);
                                }
                            });
                            futures.add(consumerFuture);
                        }
                        groupedTopics.remove(topicName.getPartitionedTopicName());
                    } else if (consumers.containsKey(tp.toString())) {
                        // Case-2: Non-partitioned topic, but has been subscribing.
                        groupedTopics.remove(topicName.getPartitionedTopicName());
                    }
                }
                // Case 3: Non-partitioned topic or Partitioned topic, but has not been subscribing.
                for (String partitionedTopic : groupedTopics) {
                    CompletableFuture consumerFuture = subscribeAsync(partitionedTopic, false);
                    consumerFuture.whenComplete((__, ex) -> {
                        if (ex != null) {
                            log.warn("[{}] Failed to subscribe to topics: {}", partitionedTopic, ex);
                        }
                    });
                    futures.add(consumerFuture);
                }
                return FutureUtil.waitForAll(futures);
            });
            return updateFuture = newFuture.exceptionally(ex -> null);
        }
    }

    @Override
    @SuppressFBWarnings
    public CompletableFuture<Void> closeAsync() {
        Timeout timeout = recheckPatternTimeout;
        if (timeout != null) {
            timeout.cancel();
            recheckPatternTimeout = null;
        }
        List<CompletableFuture<?>> closeFutures = new ArrayList<>(2);
        if (watcherFuture.isDone() && !watcherFuture.isCompletedExceptionally()) {
            TopicListWatcher watcher = watcherFuture.getNow(null);
            // watcher can be null when subscription mode is not persistent
            if (watcher != null) {
                closeFutures.add(watcher.closeAsync());
            }
        }
        closeFutures.add(updateFuture.thenCompose(__ -> super.closeAsync()));
        return FutureUtil.waitForAll(closeFutures);
    }

    @VisibleForTesting
    Timeout getRecheckPatternTimeout() {
        return recheckPatternTimeout;
    }

    private static final Logger log = LoggerFactory.getLogger(PatternMultiTopicsConsumerImpl.class);
}

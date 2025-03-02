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
import com.google.re2j.Pattern;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.validation.constraints.NotNull;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SelectiveConsumerHandler;
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

public class PatternMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> implements TimerTask,
        SelectiveConsumerHandler {
    private final Pattern topicsPattern;
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

    private PatternConsumerUpdateQueue updateTaskQueue;

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

        this.updateTaskQueue = new PatternConsumerUpdateQueue(this);
        this.recheckPatternTimeout = client.timer()
                .newTimeout(this, Math.max(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.SECONDS);
        if (subscriptionMode == Mode.PERSISTENT) {
            long watcherId = client.newTopicListWatcherId();
            new TopicListWatcher(updateTaskQueue, client, topicsPattern, watcherId,
                namespaceName, topicsHash, watcherFuture, () -> recheckTopicsChangeAfterReconnect());
            watcherFuture
               .thenAccept(__ -> recheckPatternTimeout.cancel())
               .exceptionally(ex -> {
                   log.warn("Pattern consumer [{}] unable to create topic list watcher. Falling back to only polling"
                           + " for new topics", conf.getSubscriptionName(), ex);
                   return null;
               });
        } else {
            log.debug("Pattern consumer [{}] not creating topic list watcher for subscription mode {}",
                    conf.getSubscriptionName(), subscriptionMode);
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
        updateTaskQueue.appendRecheckOp();
    }

    // TimerTask to recheck topics change, and trigger subscribe/unsubscribe based on the change.
    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }
        updateTaskQueue.appendRecheckOp();
    }

    CompletableFuture<Void> recheckTopicsChange() {
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
                        log.debug("Pattern consumer [{}] get topics under namespace {}, topics.size: {},"
                                        + " topicsHash: {}, filtered: {}",
                                PatternMultiTopicsConsumerImpl.this.getSubscription(),
                                namespaceName, getTopicsResult.getTopics().size(), getTopicsResult.getTopicsHash(),
                                getTopicsResult.isFiltered());
                        getTopicsResult.getTopics().forEach(topicName ->
                                log.debug("Get topics under namespace {}, topic: {}", namespaceName, topicName));
                    }

                    final List<String> oldTopics = new ArrayList<>(getPartitions());
                    return updateSubscriptions(topicsPattern, this::setTopicsHash, getTopicsResult,
                            topicsChangeListener, oldTopics, subscription, blockedTopics);
                }
            });
    }

    @VisibleForTesting
    static CompletableFuture<Void> updateSubscriptions(Pattern topicsPattern,
                                                       java.util.function.Consumer<String> topicsHashSetter,
                                                       GetTopicsResult getTopicsResult,
                                                       TopicsChangedListener topicsChangedListener,
                                                       List<String> oldTopics,
                                                       String subscriptionForLog) {
        return updateSubscriptions(topicsPattern, topicsHashSetter, getTopicsResult, topicsChangedListener, oldTopics,
                subscriptionForLog, Collections.emptySet());
    }

    static CompletableFuture<Void> updateSubscriptions(Pattern topicsPattern,
                                                       java.util.function.Consumer<String> topicsHashSetter,
                                                       GetTopicsResult getTopicsResult,
                                                       TopicsChangedListener topicsChangedListener,
                                                       List<String> oldTopics,
                                                       String subscriptionForLog, Set<String> blockedTopics) {
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
        Set<String> topicsAdded = TopicList.minus(newTopics, oldTopics);
        Set<String> topicsRemoved = TopicList.minus(oldTopics, newTopics);
        if (!blockedTopics.isEmpty()) {
            topicsAdded.removeAll(blockedTopics);
        }

        if (log.isDebugEnabled()) {
            log.debug("Pattern consumer [{}] Recheck pattern consumer's topics. topicsAdded: {}, topicsRemoved: {}",
                    subscriptionForLog, topicsAdded, topicsRemoved);
        }
        listenersCallback.add(topicsChangedListener.onTopicsAdded(topicsAdded));
        listenersCallback.add(topicsChangedListener.onTopicsRemoved(topicsRemoved));
        return FutureUtil.waitForAll(Collections.unmodifiableList(listenersCallback));
    }

    public Pattern getPattern() {
        return this.topicsPattern;
    }

    @VisibleForTesting
    void setTopicsHash(String topicsHash) {
        this.topicsHash = topicsHash;
    }

    @Override
    public void closeTopicConsumer(@NotNull Collection<String> topicNames) {
        if (topicNames.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No topics provided to closeTopicConsumer.", subscription);
            }
            return;
        }
        topicNames.retainAll(consumers.keySet());
        blockedTopics.addAll(topicNames);
        updateTaskQueue.appendTopicsRemovedOp(topicNames);
    }

    @Override
    public void addTopicConsumer(@NotNull Collection<String> topicNames) {
        if (topicNames.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No topics provided to closeTopicConsumer.", subscription);
            }
            return;
        }

        topicNames.removeAll(consumers.keySet());
        updateTaskQueue.appendTopicsAddedOp(topicNames);
        blockedTopics.removeAll(topicNames);
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
        closeFutures.add(updateTaskQueue.cancelAllAndWaitForTheRunningTask().thenCompose(__ -> super.closeAsync()));
        return FutureUtil.waitForAll(closeFutures);
    }

    @VisibleForTesting
    Timeout getRecheckPatternTimeout() {
        return recheckPatternTimeout;
    }

    protected void handleSubscribeOneTopicError(String topicName,
                                                Throwable error,
                                                CompletableFuture<Void> subscribeFuture) {
        subscribeFuture.completeExceptionally(error);
    }

    private static final Logger log = LoggerFactory.getLogger(PatternMultiTopicsConsumerImpl.class);
}

/**
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
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatternMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> implements TimerTask {
    private final Pattern topicsPattern;
    private final TopicsChangedListener topicsChangeListener;
    private final Mode subscriptionMode;
    protected NamespaceName namespaceName;
    private volatile Timeout recheckPatternTimeout = null;
    private volatile String topicsHash;

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

        if (this.namespaceName == null) {
            this.namespaceName = getNameSpaceFromPattern(topicsPattern);
        }
        checkArgument(getNameSpaceFromPattern(topicsPattern).toString().equals(this.namespaceName.toString()));

        this.topicsChangeListener = new PatternTopicsChangedListener();
        this.recheckPatternTimeout = client.timer()
                .newTimeout(this, Math.max(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.SECONDS);
    }

    public static NamespaceName getNameSpaceFromPattern(Pattern pattern) {
        return TopicName.get(pattern.pattern()).getNamespaceObject();
    }

    // TimerTask to recheck topics change, and trigger subscribe/unsubscribe based on the change.
    @Override
    public void run(Timeout timeout) throws Exception {
        if (timeout.isCancelled()) {
            return;
        }


        client.getLookup().getTopicsUnderNamespace(namespaceName, subscriptionMode, topicsPattern.pattern(),
                topicsHash).thenAccept(getTopicsResult -> {
            List<String> oldTopics = new ArrayList<>();
            oldTopics.addAll(getPartitionedTopics());
            getPartitions().forEach(p -> {
                TopicName t = TopicName.get(p);
                if (!t.isPartitioned() || !oldTopics.contains(t.getPartitionedTopicName())) {
                    oldTopics.add(p);
                }
            });
            topicsHash = updateSubscriptions(topicsPattern, topicsHash, namespaceName, getTopicsResult,
                    topicsChangeListener, oldTopics, topic);
        });

        // schedule the next re-check task
        this.recheckPatternTimeout = client.timer().newTimeout(PatternMultiTopicsConsumerImpl.this,
                Math.max(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.SECONDS);
    }

    static String updateSubscriptions(Pattern topicsPattern, String topicsHash, NamespaceName namespaceName,
                               GetTopicsResult getTopicsResult, TopicsChangedListener topicsChangeListener,
                               List<String> oldTopics, String dummyTopicName) {
        List<CompletableFuture<Void>> futures = new ArrayList(2);
        if (log.isDebugEnabled()) {
            log.debug("Get topics under namespace {}, topics.size: {}, topicsHash: {}, filtered: {}",
                    namespaceName, getTopicsResult.getTopics().size(), getTopicsResult.getTopicsHash(),
                    getTopicsResult.isFiltered());
            getTopicsResult.getTopics().forEach(topicName ->
                    log.debug("Get topics under namespace {}, topic: {}", namespaceName, topicName));
        }

        if (!getTopicsResult.isChanged()) {
            return topicsHash;
        }
        String newTopicsHash = getTopicsResult.getTopicsHash();

        List<String> newTopics;
        if (getTopicsResult.isFiltered()) {
            newTopics = getTopicsResult.getTopics();
        } else {
            newTopics = TopicList.filterTopics(getTopicsResult.getTopics(), topicsPattern);
        }


        futures.add(topicsChangeListener.onTopicsAdded(TopicList.minus(newTopics, oldTopics)));
        futures.add(topicsChangeListener.onTopicsRemoved(TopicList.minus(oldTopics, newTopics)));
        FutureUtil.waitForAll(futures)
            .exceptionally(ex -> {
                log.warn("[{}] Failed to recheck topics change: {}", dummyTopicName, ex.getMessage());
                return null;
            });
        return newTopicsHash;
    }

    public Pattern getPattern() {
        return this.topicsPattern;
    }

    interface TopicsChangedListener {
        // unsubscribe and delete ConsumerImpl in the `consumers` map in `MultiTopicsConsumerImpl` based on added topics
        CompletableFuture<Void> onTopicsRemoved(Collection<String> removedTopics);
        // subscribe and create a list of new ConsumerImpl, added them to the `consumers` map in
        // `MultiTopicsConsumerImpl`.
        CompletableFuture<Void> onTopicsAdded(Collection<String> addedTopics);
    }

    private class PatternTopicsChangedListener implements TopicsChangedListener {
        @Override
        public CompletableFuture<Void> onTopicsRemoved(Collection<String> removedTopics) {
            CompletableFuture<Void> removeFuture = new CompletableFuture<>();

            if (removedTopics.isEmpty()) {
                removeFuture.complete(null);
                return removeFuture;
            }

            List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(partitionedTopics.size());
            removedTopics.stream().forEach(topic -> futures.add(removeConsumerAsync(topic)));
            FutureUtil.waitForAll(futures)
                .thenAccept(finalFuture -> removeFuture.complete(null))
                .exceptionally(ex -> {
                    log.warn("[{}] Failed to subscribe topics: {}", topic, ex.getMessage());
                    removeFuture.completeExceptionally(ex);
                return null;
            });
            return removeFuture;
        }

        @Override
        public CompletableFuture<Void> onTopicsAdded(Collection<String> addedTopics) {
            CompletableFuture<Void> addFuture = new CompletableFuture<>();

            if (addedTopics.isEmpty()) {
                addFuture.complete(null);
                return addFuture;
            }

            List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(partitionedTopics.size());
            addedTopics.stream().forEach(topic -> futures.add(
                    subscribeAsync(topic, false /* createTopicIfDoesNotExist */)));
            FutureUtil.waitForAll(futures)
                .thenAccept(finalFuture -> addFuture.complete(null))
                .exceptionally(ex -> {
                    log.warn("[{}] Failed to unsubscribe topics: {}", topic, ex.getMessage());
                    addFuture.completeExceptionally(ex);
                    return null;
                });
            return addFuture;
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        Timeout timeout = recheckPatternTimeout;
        if (timeout != null) {
            timeout.cancel();
            recheckPatternTimeout = null;
        }
        return super.closeAsync();
    }

    @VisibleForTesting
    Timeout getRecheckPatternTimeout() {
        return recheckPatternTimeout;
    }

    private static final Logger log = LoggerFactory.getLogger(PatternMultiTopicsConsumerImpl.class);
}

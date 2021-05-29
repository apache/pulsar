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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatternMultiTopicsConsumerImpl<T> extends MultiTopicsConsumerImpl<T> implements TimerTask {
    private final Pattern topicsPattern;
    private final TopicsChangedListener topicsChangeListener;
    private final Mode subscriptionMode;
    protected NamespaceName namespaceName;
    private volatile Timeout recheckPatternTimeout = null;

    public PatternMultiTopicsConsumerImpl(Pattern topicsPattern,
                                          PulsarClientImpl client,
                                          ConsumerConfigurationData<T> conf,
                                          ExecutorProvider executorProvider,
                                          CompletableFuture<Consumer<T>> subscribeFuture,
                                          Schema<T> schema, Mode subscriptionMode, ConsumerInterceptors<T> interceptors) {
        super(client, conf, executorProvider, subscribeFuture, schema, interceptors,
                false /* createTopicIfDoesNotExist */);
        this.topicsPattern = topicsPattern;
        this.subscriptionMode = subscriptionMode;

        if (this.namespaceName == null) {
            this.namespaceName = getNameSpaceFromPattern(topicsPattern);
        }
        checkArgument(getNameSpaceFromPattern(topicsPattern).toString().equals(this.namespaceName.toString()));

        this.topicsChangeListener = new PatternTopicsChangedListener();
        this.recheckPatternTimeout = client.timer().newTimeout(this, Math.max(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.SECONDS);
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

        CompletableFuture<Void> recheckFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = Lists.newArrayListWithExpectedSize(2);

        client.getLookup().getTopicsUnderNamespace(namespaceName, subscriptionMode).thenAccept(topics -> {
            if (log.isDebugEnabled()) {
                log.debug("Get topics under namespace {}, topics.size: {}", namespaceName.toString(), topics.size());
                topics.forEach(topicName ->
                    log.debug("Get topics under namespace {}, topic: {}", namespaceName.toString(), topicName));
            }

            List<String> newTopics = PulsarClientImpl.topicsPatternFilter(topics, topicsPattern);
            List<String> oldTopics = Lists.newArrayList();
            oldTopics.addAll(getPartitionedTopics());
            getPartitions().forEach(p -> {
                TopicName t = TopicName.get(p);
                if (!t.isPartitioned() || !oldTopics.contains(t.getPartitionedTopicName())) {
                    oldTopics.add(p);
                }
            });

            futures.add(topicsChangeListener.onTopicsAdded(topicsListsMinus(newTopics, oldTopics)));
            futures.add(topicsChangeListener.onTopicsRemoved(topicsListsMinus(oldTopics, newTopics)));
            FutureUtil.waitForAll(futures)
                .thenAccept(finalFuture -> recheckFuture.complete(null))
                .exceptionally(ex -> {
                    log.warn("[{}] Failed to recheck topics change: {}", topic, ex.getMessage());
                    recheckFuture.completeExceptionally(ex);
                    return null;
                });
        });

        // schedule the next re-check task
        this.recheckPatternTimeout = client.timer().newTimeout(PatternMultiTopicsConsumerImpl.this,
                Math.max(1, conf.getPatternAutoDiscoveryPeriod()), TimeUnit.SECONDS);
    }

    public Pattern getPattern() {
        return this.topicsPattern;
    }

    interface TopicsChangedListener {
        // unsubscribe and delete ConsumerImpl in the `consumers` map in `MultiTopicsConsumerImpl` based on added topics.
        CompletableFuture<Void> onTopicsRemoved(Collection<String> removedTopics);
        // subscribe and create a list of new ConsumerImpl, added them to the `consumers` map in `MultiTopicsConsumerImpl`.
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
            addedTopics.stream().forEach(topic -> futures.add(subscribeAsync(topic, false /* createTopicIfDoesNotExist */)));
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

    // get topics, which are contained in list1, and not in list2
    public static List<String> topicsListsMinus(List<String> list1, List<String> list2) {
        HashSet<String> s1 = new HashSet<>(list1);
        s1.removeAll(list2);
        return s1.stream().collect(Collectors.toList());
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

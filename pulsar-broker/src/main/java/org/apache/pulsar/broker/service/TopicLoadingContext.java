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
package org.apache.pulsar.broker.service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.stats.BrokerOperabilityMetrics.TopicLoadFailureReason;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.LatencyTracer;
import org.jspecify.annotations.Nullable;

public class TopicLoadingContext extends LatencyTracer {

    public enum TopicLoadingStage {
        NAMESPACE_POLICIES("namespace policies", TopicLoadFailureReason.TIMEOUT_LOAD_NAMESPACE_POLICIES),
        TOPIC_POLICIES("topic policies", TopicLoadFailureReason.TIMEOUT_LOAD_TOPIC_POLICIES),
        OPEN_ML("open-ml", TopicLoadFailureReason.TIMEOUT_LOAD_ML),
        INITIALIZE("init", TopicLoadFailureReason.TIMEOUT_INIT),
        PRE_CREATE_COMPACTED_SUB("pre-create compacted sub", TopicLoadFailureReason.TIMEOUT_INIT),
        REPLICATION("replication", TopicLoadFailureReason.TIMEOUT_INIT),
        DEDUPLICATION("deduplication", TopicLoadFailureReason.TIMEOUT_DEDUP);

        private final String tracePoint;
        private final TopicLoadFailureReason timeoutReason;

        TopicLoadingStage(String tracePoint, TopicLoadFailureReason timeoutReason) {
            this.tracePoint = tracePoint;
            this.timeoutReason = timeoutReason;
        }
    }

    @Getter
    private final TopicName topicName;
    @Getter
    private final boolean createIfMissing;
    @Getter
    private final CompletableFuture<Optional<Topic>> topicFuture;
    @Getter
    @Setter
    @Nullable private Map<String, String> properties;
    private final AtomicReference<TopicLoadFailureReason> failureReason = new AtomicReference<>();
    private final ConcurrentHashMap<TopicLoadingStage, AtomicInteger> pendingStages = new ConcurrentHashMap<>();

    public TopicLoadingContext(TopicName topicName, boolean createIfMissing,
                               CompletableFuture<Optional<Topic>> topicFuture) {
        // The topic loading could be ended asynchronously by a timeout event, so we need a thread safe queue here
        super(new ConcurrentLinkedQueue<>(), System::nanoTime);
        this.topicName = topicName;
        this.createIfMissing = createIfMissing;
        this.topicFuture = topicFuture;
    }

    public <T> CompletableFuture<T> trace(TopicLoadingStage stage, CompletableFuture<T> future) {
        if (future.isDone()) {
            return future;
        }
        start(stage);
        return future.whenComplete((__, ___) -> {
            finish(stage);
        });
    }

    public void start(TopicLoadingStage stage) {
        pendingStages.computeIfAbsent(stage, __ -> new AtomicInteger()).incrementAndGet();
    }

    public void finish(TopicLoadingStage stage) {
        pendingStages.computeIfPresent(stage, (__, count) -> count.decrementAndGet() == 0 ? null : count);
        trace(stage.tracePoint);
    }

    public void setTopicLoadFailureReason(TopicLoadFailureReason reason) {
        failureReason.compareAndSet(null, reason);
    }

    public TopicLoadFailureReason getTopicLoadFailureReason() {
        return failureReason.get();
    }

    public TopicLoadFailureReason getTopicLoadTimeoutReason() {
        boolean namespacePoliciesPending = pendingStages.containsKey(TopicLoadingStage.NAMESPACE_POLICIES);
        boolean topicPoliciesPending = pendingStages.containsKey(TopicLoadingStage.TOPIC_POLICIES);
        if (namespacePoliciesPending != topicPoliciesPending) {
            return namespacePoliciesPending
                    ? TopicLoadFailureReason.TIMEOUT_LOAD_NAMESPACE_POLICIES
                    : TopicLoadFailureReason.TIMEOUT_LOAD_TOPIC_POLICIES;
        }
        if (pendingStages.size() != 1) {
            return TopicLoadFailureReason.TIMEOUT;
        }
        return pendingStages.keySet().iterator().next().timeoutReason;
    }
}

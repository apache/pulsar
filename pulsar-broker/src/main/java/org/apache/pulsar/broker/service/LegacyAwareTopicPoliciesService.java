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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.jspecify.annotations.NonNull;

/**
 * Routes topic policy operations to the legacy system-topic backend when a namespace already has
 * a topic-policy {@code __change_events} system topic, and otherwise to the configured backend.
 */
@CustomLog
public class LegacyAwareTopicPoliciesService implements TopicPoliciesService {

    private final AsyncLoadingCache<NamespaceName, Boolean> isLegacyNamespace;
    @VisibleForTesting
    final SystemTopicBasedTopicPoliciesService systemTopicService;
    private final TopicPoliciesService configuredService;

    public LegacyAwareTopicPoliciesService(PulsarService pulsar,
                                           SystemTopicBasedTopicPoliciesService systemTopicService,
                                           TopicPoliciesService configuredService) {
        // Generally, we only need to check if the __change_events topic exists once because the __change_events topic
        // should only be created by broker before the upgrade, where `SystemTopicBasedTopicPoliciesService` is
        // configured as the topic policies service.
        this.isLegacyNamespace = Caffeine.newBuilder().expireAfterWrite(Duration.ofHours(1))
                .buildAsync(new AsyncCacheLoader<>() {
                    @NonNull
                    @Override
                    public CompletableFuture<? extends Boolean> asyncLoad(NamespaceName key,
                                                                          @NonNull Executor executor) {
                        return NamespaceEventsSystemTopicFactory.checkSystemTopicExists(key, EventType.TOPIC_POLICY,
                                pulsar);
                    }
                });
        this.systemTopicService = systemTopicService;
        this.configuredService = configuredService;
        if (configuredService instanceof SystemTopicBasedTopicPoliciesService) {
            throw new IllegalArgumentException(
                    "configuredService should not be an instance of SystemTopicBasedTopicPoliciesService");
        }
    }

    @Override
    public void start(PulsarService pulsarService) {
        // We should not call `systemTopicService.start()`, which just registers a namespace bundle listener to create
        // a reader on `<namespace>/__change_events` when the namespace's bundle is loaded firstly. It's just an
        // optimization to create the reader before loading any topic. However, it could create a reader on a namespace
        // that does not even have the __change_events topic.
        configuredService.start(pulsarService);
    }

    @Override
    public void close() throws Exception {
        try {
            configuredService.close();
        } finally {
            systemTopicService.close();
        }
    }

    @Override
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.getTopicPoliciesAsync(topicName, type));
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, boolean isGlobalPolicy,
                                                            boolean skipUpdateWhenTopicPolicyDoesntExist,
                                                            Consumer<TopicPolicies> policyUpdater) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.updateTopicPoliciesAsync(topicName, isGlobalPolicy,
                        skipUpdateWhenTopicPolicyDoesntExist, policyUpdater));
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.deleteTopicPoliciesAsync(topicName));
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName,
                                                            boolean keepGlobalPoliciesAfterDeleting) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.deleteTopicPoliciesAsync(topicName,
                        keepGlobalPoliciesAfterDeleting));
    }

    @Override
    public CompletableFuture<Boolean> registerListenerAsync(TopicName topicName, TopicPolicyListener listener) {
        return resolveService(topicName.getNamespaceObject())
                .thenCompose(service -> service.registerListenerAsync(topicName, listener));
    }

    @Override
    public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
        throw new RuntimeException("should not be called");
    }

    @Override
    public void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
        configuredService.unregisterListener(topicName, listener);
        systemTopicService.unregisterListener(topicName, listener);
    }

    @VisibleForTesting
    CompletableFuture<TopicPoliciesService> resolveService(NamespaceName namespace) {
        return isLegacyNamespace.get(namespace)
                .thenApply(isLegacy -> isLegacy ? systemTopicService : configuredService);
    }
}

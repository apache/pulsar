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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;

/**
 * Routes topic policy operations to the legacy system-topic backend when a namespace already has
 * a topic-policy {@code __change_events} system topic, and otherwise to the configured backend.
 */
@CustomLog
public class LegacyAwareTopicPoliciesService implements TopicPoliciesService {

    private final PulsarService pulsar;
    private final SystemTopicBasedTopicPoliciesService systemTopicService;
    private final TopicPoliciesService configuredService;

    public LegacyAwareTopicPoliciesService(PulsarService pulsar,
                                           SystemTopicBasedTopicPoliciesService systemTopicService,
                                           TopicPoliciesService configuredService) {
        this.pulsar = pulsar;
        this.systemTopicService = systemTopicService;
        this.configuredService = configuredService;
    }

    @Override
    public void start(PulsarService pulsarService) {
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
    public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
        boolean configuredRegistered = configuredService.registerListener(topicName, listener);
        boolean systemTopicRegistered = systemTopicService.registerListener(topicName, listener);
        return configuredRegistered || systemTopicRegistered;
    }

    @Override
    public void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
        configuredService.unregisterListener(topicName, listener);
        systemTopicService.unregisterListener(topicName, listener);
    }

    private CompletableFuture<TopicPoliciesService> resolveService(NamespaceName namespace) {
        return isLegacy(namespace).thenApply(isLegacy -> isLegacy ? systemTopicService : configuredService);
    }

    private CompletableFuture<Boolean> isLegacy(NamespaceName namespace) {
        return NamespaceEventsSystemTopicFactory.checkSystemTopicExists(namespace, EventType.TOPIC_POLICY, pulsar);
    }
}

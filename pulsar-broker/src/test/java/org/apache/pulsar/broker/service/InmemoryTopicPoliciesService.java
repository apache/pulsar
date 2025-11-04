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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;

public class InmemoryTopicPoliciesService implements TopicPoliciesService {
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(new DefaultThreadFactory("InmemoryTopicPoliciesService"));
    private final Map<TopicName, TopicPolicies> cache = new HashMap<>();
    private final Map<TopicName, List<TopicPolicyListener>> listeners = new HashMap<>();

    @Override
    public synchronized CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        cache.remove(topicName);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName,
                                                                         boolean isGlobalPolicy,
                                                                         boolean skipUpdateWhenTopicPolicyDoesntExist,
                                                                         Consumer<TopicPolicies> policyUpdater) {
        return CompletableFuture.runAsync(() -> {
            final var existingPolicies = cache.get(topicName);
            if (existingPolicies == null && skipUpdateWhenTopicPolicyDoesntExist) {
                return; // No existing policies and skip update
            }
            final TopicPolicies newPolicies = existingPolicies == null
                    ? createTopicPolicy(isGlobalPolicy)
                    : existingPolicies.clone();
            policyUpdater.accept(newPolicies);
            cache.put(topicName, newPolicies);
            List<TopicPolicyListener> listeners;
            synchronized (this) {
                listeners = this.listeners.getOrDefault(topicName, List.of());
            }
            for (var listener : listeners) {
                listener.onUpdate(newPolicies);
            }
        }, executor);
    }

    private static TopicPolicies createTopicPolicy(boolean isGlobalPolicy) {
        TopicPolicies topicPolicies = new TopicPolicies();
        topicPolicies.setIsGlobal(isGlobalPolicy);
        return topicPolicies;
    }

    @Override
    public synchronized CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(
            TopicName topicName, GetType type) {
        return CompletableFuture.completedFuture(Optional.ofNullable(cache.get(topicName)));
    }

    @Override
    public synchronized boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
        listeners.computeIfAbsent(topicName, __ -> new ArrayList<>()).add(listener);
        return true;
    }

    @Override
    public synchronized void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
        listeners.get(topicName).remove(listener);
    }

    synchronized boolean containsKey(TopicName topicName) {
        return cache.containsKey(topicName);
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
    }
}

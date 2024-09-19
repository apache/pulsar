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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;

public class InmemoryTopicPoliciesService implements TopicPoliciesService {

    private final Map<TopicName, TopicPolicies> cache = new HashMap<>();
    private final Map<TopicName, List<TopicPolicyListener>> listeners = new HashMap<>();

    @Override
    public synchronized CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        cache.remove(topicName);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies) {
        final var existingPolicies = cache.get(topicName);
        if (existingPolicies != policies) {
            cache.put(topicName, policies);
            CompletableFuture.runAsync(() -> {
                final TopicPolicies latestPolicies;
                final List<TopicPolicyListener> listeners;
                synchronized (InmemoryTopicPoliciesService.this) {
                    latestPolicies = cache.get(topicName);
                    listeners = this.listeners.getOrDefault(topicName, List.of());
                }
                for (var listener : listeners) {
                    listener.onUpdate(latestPolicies);
                }
            });
        }
        return CompletableFuture.completedFuture(null);
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
}

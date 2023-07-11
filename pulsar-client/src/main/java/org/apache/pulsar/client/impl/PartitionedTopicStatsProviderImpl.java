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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicStatsProvider;
import org.apache.pulsar.common.policies.data.TopicInternalStatsInfo;
import org.apache.pulsar.common.policies.data.TopicStatsInfo;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TopicStatsProvider} implementation to fetch stats for group of partitions.
 *
 */
public class PartitionedTopicStatsProviderImpl implements TopicStatsProvider {

    private static final Logger log = LoggerFactory.getLogger(PartitionedTopicStatsProviderImpl.class);
    private final ConcurrentOpenHashMap<String, TopicStatsProvider> statsProviders;

    public PartitionedTopicStatsProviderImpl(String topicName) {
        this.statsProviders = ConcurrentOpenHashMap.<String, TopicStatsProvider> newBuilder().build();
    }

    public void addStatsProvider(String partitionName, TopicStatsProvider provider) {
        statsProviders.put(partitionName, provider);
    }

    @Override
    public TopicStatsInfo getStats() throws PulsarClientException {
        try {
            return getStatsAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<TopicStatsInfo> getStatsAsync() {
        TopicStatsInfo statsInfo = new TopicStatsInfo();
        return getTopicStats(statsInfo, (provider) -> provider.getStatsAsync(), (stats) -> {
            statsInfo.getPartitions().putAll(stats.getPartitions());
        });
    }

    @Override
    public TopicInternalStatsInfo getInternalStats() throws PulsarClientException {
        try {
            return getInternalStatsAsync().get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }
    }

    @Override
    public CompletableFuture<TopicInternalStatsInfo> getInternalStatsAsync() {
        TopicInternalStatsInfo statsInfo = new TopicInternalStatsInfo();
        return getTopicStats(statsInfo, (provider) -> provider.getInternalStatsAsync(),
                (stats) -> statsInfo.getPartitions().putAll(stats.getPartitions()));
    }

    private <R> CompletableFuture<R> getTopicStats(R stats,
            Function<TopicStatsProvider, CompletableFuture<R>> providerStats, Consumer<R> statsUpdater) {
        CompletableFuture<R> statsResult = new CompletableFuture<>();
        AtomicInteger count = new AtomicInteger((int) statsProviders.size());
        statsProviders.forEach((partition, provider) -> {
            providerStats.apply(provider).thenAccept(s -> {
                statsUpdater.accept(s);
                if (count.decrementAndGet() == 0) {
                    statsResult.complete(stats);
                }
            }).exceptionally(ex -> {
                log.warn("Failed to fetch stats for topic {}", partition, ex.getCause());
                statsResult.completeExceptionally(ex.getCause());
                return null;
            });
        });
        return statsResult;
    }
}

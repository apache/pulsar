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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicStatsProvider;
import org.apache.pulsar.common.api.proto.CommandTopicStats.StatsType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TopicInternalStatsInfo;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.TopicStatsInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TopicStatsProvider implementation that provides topic's stats and internal-stats.
 */
public class TopicStatsProviderImpl implements TopicStatsProvider {

    private static final Logger log = LoggerFactory.getLogger(TopicStatsProviderImpl.class);
    private final PulsarClientImpl client;
    private final ConnectionHandler cnxHandler;
    private final String topicName;
    private final boolean isPersistent;

    public TopicStatsProviderImpl(PulsarClientImpl client, ConnectionHandler cnxHandler, String topicName) {
        this.client = client;
        this.cnxHandler = cnxHandler;
        this.topicName = topicName;
        this.isPersistent = TopicName.get(topicName).isPersistent();
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
        Class<? extends TopicStats> clazz = isPersistent ? TopicStats.class : NonPersistentTopicStats.class;
        return sendRequest(client, cnxHandler, topicName, StatsType.STATS, clazz).thenApply(stats -> {
            TopicStatsInfo info = new TopicStatsInfo();
            info.getPartitions().put(topicName, stats);
            return info;
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
        return sendRequest(client, cnxHandler, topicName, StatsType.STATS_INTERNAL, PersistentTopicInternalStats.class)
                .thenApply(stats -> {
                    TopicInternalStatsInfo info = new TopicInternalStatsInfo();
                    info.getPartitions().put(topicName, stats);
                    return info;
                });
    }

    public static <T> CompletableFuture<T> sendRequest(PulsarClientImpl client, ConnectionHandler cnxHandler,
            String topicName, StatsType statsType, Class<T> clazz) {
        long requestId = client.newRequestId();
        ByteBuf cmd = Commands.newStats(topicName, statsType, requestId);
        CompletableFuture<T> result = new CompletableFuture<>();
        ClientCnx cnx = cnxHandler.cnx();
        cnx.newStatsRequest(cmd, requestId).thenAccept(statsData -> {
            try {
                result.complete(ObjectMapperFactory.getMapper().reader().readValue(statsData, clazz));
            } catch (IOException e) {
                log.warn("Failed to parse {} for topic {}, response = {} - {}", statsType, topicName, statsData,
                        e.getMessage());
                result.completeExceptionally(new PulsarClientException.InvalidMessageException(e.getMessage()));
            }
        }).exceptionally(ex -> {
            result.completeExceptionally(new PulsarClientException(ex.getMessage()));
            return null;
        });
        return result;
    }

}

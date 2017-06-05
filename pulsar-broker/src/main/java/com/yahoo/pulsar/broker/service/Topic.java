/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service;

import java.util.concurrent.CompletableFuture;

import com.yahoo.pulsar.broker.stats.ClusterReplicationMetrics;
import com.yahoo.pulsar.broker.stats.NamespaceStats;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.common.policies.data.BacklogQuota;
import com.yahoo.pulsar.common.policies.data.PersistentTopicInternalStats;
import com.yahoo.pulsar.common.policies.data.PersistentTopicStats;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;
import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashMap;
import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashSet;
import com.yahoo.pulsar.utils.StatsOutputStream;

import io.netty.buffer.ByteBuf;

public interface Topic {

    public interface PublishCallback {
        void completed(Exception e, long ledgerId, long entryId);
    }

    void publishMessage(ByteBuf headersAndPayload, PublishCallback callback);

    void addProducer(Producer producer) throws BrokerServiceException;

    void removeProducer(Producer producer);

    CompletableFuture<Consumer> subscribe(ServerCnx cnx, String subscriptionName, long consumerId, SubType subType,
            int priorityLevel, String consumerName, boolean isDurable, MessageId startMessageId);
    
    CompletableFuture<Subscription> createSubscription(String subscriptionName);

    CompletableFuture<Void> unsubscribe(String subName);

    ConcurrentOpenHashMap<String, ? extends Subscription> getSubscriptions();

    CompletableFuture<Void> delete();

    ConcurrentOpenHashSet<Producer> getProducers();

    String getName();

    CompletableFuture<Void> checkReplication();

    CompletableFuture<Void> close();

    void checkGC(int gcInterval);

    void checkMessageExpiry();

    CompletableFuture<Void> onPoliciesUpdate(Policies data);

    boolean isBacklogQuotaExceeded(String producerName);
    
    BacklogQuota getBacklogQuota();

    void updateRates(NamespaceStats nsStats, NamespaceBundleStats currentBundleStats,
            StatsOutputStream topicStatsStream, ClusterReplicationMetrics clusterReplicationMetrics,
            String namespaceName);

    Subscription getSubscription(String subscription);

    ConcurrentOpenHashMap<String, Replicator> getReplicators();

    PersistentTopicStats getStats();

    PersistentTopicInternalStats getInternalStats();
}

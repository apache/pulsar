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
package org.apache.pulsar.broker.service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.NamespaceStats;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;

import io.netty.buffer.ByteBuf;

public interface Topic {

    interface PublishContext {

        default String getProducerName() {
            return null;
        }

        default long getSequenceId() {
            return -1L;
        }

        default void setOriginalProducerName(String originalProducerName) {
        }

        default void setOriginalSequenceId(long originalSequenceId) {
        }

        /**
         * Return the producer name for the original producer.
         *
         * For messages published locally, this will return the same local producer name, though in case of replicated
         * messages, the original producer name will differ
         */
        default String getOriginalProducerName() {
            return null;
        }

        default long getOriginalSequenceId() {
            return -1L;
        }

        void completed(Exception e, long ledgerId, long entryId);

        default long getHighestSequenceId() {
            return  -1L;
        }

        default void setOriginalHighestSequenceId(long originalHighestSequenceId) {

        }

        default long getOriginalHighestSequenceId() {
            return  -1L;
        }
    }

    void publishMessage(ByteBuf headersAndPayload, PublishContext callback);

    void addProducer(Producer producer) throws BrokerServiceException;

    void removeProducer(Producer producer);

    /**
     * record add-latency
     */
    void recordAddLatency(long latency, TimeUnit unit);

    CompletableFuture<Consumer> subscribe(ServerCnx cnx, String subscriptionName, long consumerId, SubType subType,
            int priorityLevel, String consumerName, boolean isDurable, MessageId startMessageId,
            Map<String, String> metadata, boolean readCompacted, InitialPosition initialPosition,
            long startMessageRollbackDurationSec, boolean replicateSubscriptionState, PulsarApi.KeySharedMeta keySharedMeta);

    CompletableFuture<Subscription> createSubscription(String subscriptionName, InitialPosition initialPosition,
            boolean replicateSubscriptionState);

    CompletableFuture<Void> unsubscribe(String subName);

    ConcurrentOpenHashMap<String, ? extends Subscription> getSubscriptions();

    CompletableFuture<Void> delete();

    Map<String, Producer> getProducers();

    String getName();

    CompletableFuture<Void> checkReplication();

    CompletableFuture<Void> close(boolean closeWithoutWaitingClientDisconnect);

    void checkGC(int maxInactiveDurationInSec, InactiveTopicDeleteMode deleteMode);

    void checkInactiveSubscriptions();

    void checkMessageExpiry();

    void checkMessageDeduplicationInfo();

    void checkTopicPublishThrottlingRate();

    void incrementPublishCount(int numOfMessages, long msgSizeInBytes);

    void resetTopicPublishCountAndEnableReadIfRequired();

    void resetBrokerPublishCountAndEnableReadIfRequired(boolean doneReset);

    boolean isPublishRateExceeded();

    CompletableFuture<Void> onPoliciesUpdate(Policies data);

    boolean isBacklogQuotaExceeded(String producerName);

    boolean isEncryptionRequired();

    boolean getSchemaValidationEnforced();

    boolean isReplicated();

    BacklogQuota getBacklogQuota();

    void updateRates(NamespaceStats nsStats, NamespaceBundleStats currentBundleStats,
            StatsOutputStream topicStatsStream, ClusterReplicationMetrics clusterReplicationMetrics,
            String namespaceName, boolean hydratePublishers);

    Subscription getSubscription(String subscription);

    ConcurrentOpenHashMap<String, ? extends Replicator> getReplicators();

    TopicStats getStats(boolean getPreciseBacklog);

    PersistentTopicInternalStats getInternalStats();

    Position getLastPosition();

    CompletableFuture<MessageId> getLastMessageId();

    /**
     * Whether a topic has had a schema defined for it.
     */
    CompletableFuture<Boolean> hasSchema();

    /**
     * Add a schema to the topic. This will fail if the new schema is incompatible with the current
     * schema.
     */
    CompletableFuture<SchemaVersion> addSchema(SchemaData schema);

    /**
     * Delete the schema if this topic has a schema defined for it.
     */
    CompletableFuture<SchemaVersion> deleteSchema();

    /**
     * Check if schema is compatible with current topic schema.
     */
    CompletableFuture<Void> checkSchemaCompatibleForConsumer(SchemaData schema);

    /**
     * If the topic is idle (no producers, no entries, no subscribers and no existing schema),
     * add the passed schema to the topic. Otherwise, check that the passed schema is compatible
     * with what the topic already has.
     */
    CompletableFuture<Void> addSchemaIfIdleOrCheckCompatible(SchemaData schema);

    CompletableFuture<Void> deleteForcefully();

    default Optional<DispatchRateLimiter> getDispatchRateLimiter() {
        return Optional.empty();
    }
}

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

package org.apache.pulsar.broker;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ActiveManagedCursorContainerImpl;
import org.apache.bookkeeper.mledger.impl.MockManagedCursor;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.AnalyzeBacklogResult;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.GetStatsOptions;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.SubscriptionOption;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicAttributes;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.NamespaceStats;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;

@Slf4j
public class MockPersistentDispatcher extends AbstractPersistentDispatcherMultipleConsumers {

    private final MockManagedCursor cursor;
    private final MockSubscription subscription;
    private final MockTopic topic;

    private MockPersistentDispatcher(MockTopic topic, MockSubscription subscription,
                                     MockManagedCursor cursor) {
        super(subscription, new ServiceConfiguration());
        this.topic = topic;
        this.subscription = subscription;
        this.cursor = cursor;
    }

    public static MockPersistentDispatcher create() throws Exception {
        // LocalBookkeeperEnsemble bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        // bkEnsemble.start();
        //
        // // Start broker
        // ServiceConfiguration config = new ServiceConfiguration();
        // config.setClusterName("use");
        // config.setWebServicePort(Optional.of(0));
        // config.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        // config.setBrokerShutdownTimeoutMs(0L);
        // config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        // config.setBrokerServicePort(Optional.of(0));
        // config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        // config.setBrokerServicePortTls(Optional.of(0));
        // config.setWebServicePortTls(Optional.of(0));
        // config.setAdvertisedAddress("localhost");
        //
        // @Cleanup
        // PulsarService pulsarService = new PulsarService(config);
        // pulsarService.start();
        //
        // ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        // MockManagedCursor cursor = MockManagedCursor.createCursor(container, "cursor",
        //         PositionFactory.create(0, 1));
        //
        //
        //
        // TopicName topicName = TopicName.get("persistent://pulsar/default/ns/topic");
        // PersistentTopic topic = new PersistentTopic(topicName.toString(), cursor.getManagedLedger(),
        //         pulsarService.getBrokerService());
        //
        // PersistentSubscription subscription = new PersistentSubscription(topic, "sub", cursor, false);
        //
        // return new MockPersistentDispatcher(topic, subscription, cursor);


        ActiveManagedCursorContainerImpl container = new ActiveManagedCursorContainerImpl();
        MockManagedCursor cursor = MockManagedCursor.createCursor(container, "test-cursor",
                PositionFactory.create(0, 0));

        MockTopic topic = new MockTopic();
        MockSubscription subscription = new MockSubscription(topic);

        return new MockPersistentDispatcher(topic, subscription, cursor);
    }

    @Override
    public void unBlockDispatcherOnUnackedMsgs() {

    }

    @Override
    public void readMoreEntriesAsync() {

    }

    @Override
    protected boolean isConsumersExceededOnSubscription() {
        return false;
    }

    @Override
    protected void reScheduleRead() {

    }

    @Override
    public String getName() {
        return topic.getName() + " / " + Codec.decode(cursor.getName());
    }

    @Override
    public boolean isBlockedDispatcherOnUnackedMsgs() {
        return false;
    }

    @Override
    public int getTotalUnackedMessages() {
        return 0;
    }

    @Override
    public void blockDispatcherOnUnackedMsgs() {

    }

    @Override
    public long getNumberOfMessagesInReplay() {
        return 0;
    }

    @Override
    public boolean isHavePendingRead() {
        return false;
    }

    @Override
    public boolean isHavePendingReplayRead() {
        return false;
    }

    @Override
    public ManagedCursor getCursor() {
        return cursor;
    }

    @Override
    public Topic getTopic() {
        return topic;
    }

    @Override
    public Subscription getSubscription() {
        return subscription;
    }

    @Override
    public long getDelayedTrackerMemoryUsage() {
        return 0;
    }

    @Override
    public Map<String, TopicMetricBean> getBucketDelayedIndexStats() {
        return Map.of();
    }

    @Override
    public boolean isClassic() {
        return false;
    }

    @Override
    public void readEntriesComplete(List<Entry> entries, Object ctx) {

    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {

    }

    @Override
    public boolean isConsumerAvailable(Consumer consumer) {
        return false;
    }

    @Override
    public CompletableFuture<Void> addConsumer(Consumer consumer) {
        return null;
    }

    @Override
    public void removeConsumer(Consumer consumer) throws BrokerServiceException {

    }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {

    }

    @Override
    public CompletableFuture<Void> close(boolean disconnectClients,
                                         Optional<BrokerLookupData> assignedBrokerLookupData) {
        return null;
    }

    @Override
    public CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
        return null;
    }

    @Override
    public CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor,
                                                          Optional<BrokerLookupData> assignedBrokerLookupData) {
        return null;
    }

    @Override
    public void reset() {

    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {

    }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, List<Position> positions) {

    }

    @Override
    public void addUnAckedMessages(int unAckMessages) {

    }

    @Override
    public RedeliveryTracker getRedeliveryTracker() {
        return null;
    }

    private static class MockBrokerService extends BrokerService {

        public MockBrokerService(PulsarService pulsarService) throws Exception {
            super(pulsarService, null);
        }

        public static MockBrokerService create() {
            ServiceConfiguration config = new ServiceConfiguration();
            config.setClusterName("use");
            config.setWebServicePort(Optional.of(0));
            config.setMetadataStoreUrl("zk:127.0.0.1:2181");
            // config.setBrokerShutdownTimeoutMs(0L);
            // config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            config.setBrokerServicePort(Optional.of(0));
            config.setBrokerServicePortTls(Optional.of(0));
            config.setWebServicePortTls(Optional.of(0));
            config.setAdvertisedAddress("localhost");
            try (PulsarService pulsarService = new PulsarService(config)) {
                return new MockBrokerService(pulsarService);
            } catch (Exception e) {
                log.warn("Failed to create MockBrokerService", e);
                e.printStackTrace();
            }
            return null;
        }
    }

    private static class MockSubscription implements Subscription {
        private final Topic topic;

        public MockSubscription(Topic topic) {
            this.topic = topic;
        }

        @Override
        public BrokerInterceptor interceptor() {
            return null;
        }

        @Override
        public Topic getTopic() {
            return topic;
        }

        @Override
        public String getName() {
            return "mock-subscription";
        }

        @Override
        public CompletableFuture<Void> addConsumer(Consumer consumer) {
            return null;
        }

        @Override
        public void removeConsumer(Consumer consumer, boolean isResetCursor) throws BrokerServiceException {

        }

        @Override
        public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {

        }

        @Override
        public void acknowledgeMessage(List<Position> positions, CommandAck.AckType ackType,
                                       Map<String, Long> properties) {

        }

        @Override
        public String getTopicName() {
            return topic.getName();
        }

        @Override
        public boolean isReplicated() {
            return false;
        }

        @Override
        public Dispatcher getDispatcher() {
            return null;
        }

        @Override
        public long getNumberOfEntriesInBacklog(boolean getPreciseBacklog) {
            return 0;
        }

        @Override
        public List<Consumer> getConsumers() {
            return List.of();
        }

        @Override
        public CompletableFuture<Void> delete() {
            return null;
        }

        @Override
        public CompletableFuture<Void> deleteForcefully() {
            return null;
        }

        @Override
        public CompletableFuture<Void> disconnect(Optional<BrokerLookupData> assignedBrokerLookupData) {
            return null;
        }

        @Override
        public CompletableFuture<Void> close(boolean disconnectConsumers,
                                             Optional<BrokerLookupData> assignedBrokerLookupData) {
            return null;
        }

        @Override
        public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
            return null;
        }

        @Override
        public CompletableFuture<Void> doUnsubscribe(Consumer consumer, boolean forcefully) {
            return null;
        }

        @Override
        public CompletableFuture<Void> clearBacklog() {
            return null;
        }

        @Override
        public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
            return null;
        }

        @Override
        public CompletableFuture<Void> resetCursor(long timestamp) {
            return null;
        }

        @Override
        public CompletableFuture<Void> resetCursor(Position position) {
            return null;
        }

        @Override
        public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
            return null;
        }

        @Override
        public void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {

        }

        @Override
        public void redeliverUnacknowledgedMessages(Consumer consumer, List<Position> positions) {

        }

        @Override
        public void markTopicWithBatchMessagePublished() {

        }

        @Override
        public double getExpiredMessageRate() {
            return 0;
        }

        @Override
        public CommandSubscribe.SubType getType() {
            return null;
        }

        @Override
        public String getTypeString() {
            return "";
        }

        @Override
        public void addUnAckedMessages(int unAckMessages) {

        }

        @Override
        public Map<String, String> getSubscriptionProperties() {
            return Map.of();
        }

        @Override
        public CompletableFuture<Void> updateSubscriptionProperties(Map<String, String> subscriptionProperties) {
            return null;
        }

        @Override
        public boolean isSubscriptionMigrated() {
            return false;
        }

        @Override
        public CompletableFuture<Void> endTxn(long txnidMostBits, long txnidLeastBits,
                                              int txnAction, long lowWaterMark) {
            return null;
        }

        @Override
        public CompletableFuture<AnalyzeBacklogResult> analyzeBacklog(Optional<Position> position) {
            return null;
        }

        @Override
        public boolean expireMessages(Position position) {
            return false;
        }

        @Override
        public boolean expireMessages(int messageTTLInSeconds) {
            return false;
        }

        @Override
        public CompletableFuture<Boolean> expireMessagesAsync(int messageTTLInSeconds) {
            return null;
        }
    }

    // 简单的Mock Topic实现
    private static class MockTopic implements Topic {
        @Override
        public CompletableFuture<Void> initialize() {
            return null;
        }

        @Override
        public void publishMessage(ByteBuf headersAndPayload, PublishContext callback) {

        }

        @Override
        public CompletableFuture<Optional<Long>> addProducer(Producer producer,
                                                             CompletableFuture<Void> producerQueuedFuture) {
            return null;
        }

        @Override
        public void removeProducer(Producer producer) {

        }

        @Override
        public CompletableFuture<Void> checkIfTransactionBufferRecoverCompletely() {
            return null;
        }

        @Override
        public void recordAddLatency(long latency, TimeUnit unit) {

        }

        @Override
        public long increasePublishLimitedTimes() {
            return 0;
        }

        @Override
        public CompletableFuture<Consumer> subscribe(TransportCnx cnx, String subscriptionName,
                                                     long consumerId, CommandSubscribe.SubType subType,
                                                     int priorityLevel, String consumerName, boolean isDurable,
                                                     MessageId startMessageId, Map<String, String> metadata,
                                                     boolean readCompacted,
                                                     CommandSubscribe.InitialPosition initialPosition,
                                                     long startMessageRollbackDurationSec,
                                                     boolean replicateSubscriptionState, KeySharedMeta keySharedMeta) {
            return null;
        }

        @Override
        public CompletableFuture<Consumer> subscribe(SubscriptionOption option) {
            return null;
        }

        @Override
        public CompletableFuture<Subscription> createSubscription(String subscriptionName,
                                                                  CommandSubscribe.InitialPosition initialPosition,
                                                                  boolean replicateSubscriptionState,
                                                                  Map<String, String> properties) {
            return null;
        }

        @Override
        public CompletableFuture<Void> unsubscribe(String subName) {
            return null;
        }

        @Override
        public Map<String, ? extends Subscription> getSubscriptions() {
            return Map.of();
        }

        @Override
        public CompletableFuture<Void> delete() {
            return null;
        }

        @Override
        public Map<String, Producer> getProducers() {
            return Map.of();
        }

        @Override
        public String getName() {
            return "mock-topic";
        }

        @Override
        public CompletableFuture<Void> checkReplication() {
            return null;
        }

        @Override
        public CompletableFuture<Void> close(boolean closeWithoutWaitingClientDisconnect) {
            return null;
        }

        @Override
        public CompletableFuture<Void> close(boolean disconnectClients, boolean closeWithoutWaitingClientDisconnect) {
            return null;
        }

        @Override
        public void checkGC() {

        }

        @Override
        public CompletableFuture<Void> checkClusterMigration() {
            return null;
        }

        @Override
        public void checkInactiveSubscriptions() {

        }

        @Override
        public void checkBackloggedCursors() {

        }

        @Override
        public void checkCursorsToCacheEntries() {

        }

        @Override
        public void checkDeduplicationSnapshot() {

        }

        @Override
        public void checkMessageExpiry() {

        }

        @Override
        public void checkMessageDeduplicationInfo() {

        }

        @Override
        public void incrementPublishCount(Producer producer, int numOfMessages, long msgSizeInBytes) {

        }

        @Override
        public boolean shouldProducerMigrate() {
            return false;
        }

        @Override
        public boolean isReplicationBacklogExist() {
            return false;
        }

        @Override
        public CompletableFuture<Void> onPoliciesUpdate(Policies data) {
            return null;
        }

        @Override
        public CompletableFuture<Void> checkBacklogQuotaExceeded(String producerName,
                                                                 BacklogQuota.BacklogQuotaType backlogQuotaType) {
            return null;
        }

        @Override
        public boolean isEncryptionRequired() {
            return false;
        }

        @Override
        public boolean getSchemaValidationEnforced() {
            return false;
        }

        @Override
        public boolean isReplicated() {
            return false;
        }

        @Override
        public boolean isShadowReplicated() {
            return false;
        }

        @Override
        public EntryFilters getEntryFiltersPolicy() {
            return null;
        }

        @Override
        public List<EntryFilter> getEntryFilters() {
            return List.of();
        }

        @Override
        public BacklogQuota getBacklogQuota(BacklogQuota.BacklogQuotaType backlogQuotaType) {
            return null;
        }

        @Override
        public long getBestEffortOldestUnacknowledgedMessageAgeSeconds() {
            return 0;
        }

        @Override
        public void updateRates(NamespaceStats nsStats, NamespaceBundleStats currentBundleStats,
                                StatsOutputStream topicStatsStream,
                                ClusterReplicationMetrics clusterReplicationMetrics,
                                String namespaceName, boolean hydratePublishers) {

        }

        @Override
        public Subscription getSubscription(String subscription) {
            return null;
        }

        @Override
        public Map<String, ? extends Replicator> getReplicators() {
            return Map.of();
        }

        @Override
        public Map<String, ? extends Replicator> getShadowReplicators() {
            return Map.of();
        }

        @Override
        public TopicStatsImpl getStats(boolean getPreciseBacklog, boolean subscriptionBacklogSize,
                                       boolean getEarliestTimeInBacklog) {
            return null;
        }

        @Override
        public TopicStatsImpl getStats(GetStatsOptions getStatsOptions) {
            return null;
        }

        @Override
        public CompletableFuture<? extends TopicStatsImpl> asyncGetStats(boolean getPreciseBacklog,
                                                                         boolean subscriptionBacklogSize,
                                                                         boolean getEarliestTimeInBacklog) {
            return null;
        }

        @Override
        public CompletableFuture<? extends TopicStatsImpl> asyncGetStats(GetStatsOptions getStatsOptions) {
            return null;
        }

        @Override
        public CompletableFuture<PersistentTopicInternalStats> getInternalStats(boolean includeLedgerMetadata) {
            return null;
        }

        @Override
        public Position getLastPosition() {
            return null;
        }

        @Override
        public CompletableFuture<MessageId> getLastMessageId() {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> hasSchema() {
            return null;
        }

        @Override
        public CompletableFuture<SchemaVersion> addSchema(SchemaData schema) {
            return null;
        }

        @Override
        public CompletableFuture<SchemaVersion> deleteSchema() {
            return null;
        }

        @Override
        public CompletableFuture<Void> checkSchemaCompatibleForConsumer(SchemaData schema) {
            return null;
        }

        @Override
        public CompletableFuture<Void> addSchemaIfIdleOrCheckCompatible(SchemaData schema) {
            return null;
        }

        @Override
        public CompletableFuture<Void> deleteForcefully() {
            return null;
        }

        @Override
        public boolean isPersistent() {
            return false;
        }

        @Override
        public boolean isTransferring() {
            return false;
        }

        @Override
        public void publishTxnMessage(TxnID txnID, ByteBuf headersAndPayload, PublishContext publishContext) {

        }

        @Override
        public CompletableFuture<Void> endTxn(TxnID txnID, int txnAction, long lowWaterMark) {
            return null;
        }

        @Override
        public CompletableFuture<Void> truncate() {
            return null;
        }

        @Override
        public BrokerService getBrokerService() {
            return MockBrokerService.create();
        }

        @Override
        public HierarchyTopicPolicies getHierarchyTopicPolicies() {
            return null;
        }

        @Override
        public TopicAttributes getTopicAttributes() {
            return null;
        }
    }
}

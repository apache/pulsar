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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.api.proto.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.ReplicatedSubscriptionsSnapshot;

public interface Subscription {

    BrokerInterceptor interceptor();

    Topic getTopic();

    String getName();

    CompletableFuture<Void> addConsumer(Consumer consumer);

    default void removeConsumer(Consumer consumer) throws BrokerServiceException {
        removeConsumer(consumer, false);
    }

    void removeConsumer(Consumer consumer, boolean isResetCursor) throws BrokerServiceException;

    void consumerFlow(Consumer consumer, int additionalNumberOfMessages);

    void acknowledgeMessage(List<Position> positions, AckType ackType, Map<String, Long> properties);

    String getTopicName();

    boolean isReplicated();

    Dispatcher getDispatcher();

    long getNumberOfEntriesInBacklog(boolean getPreciseBacklog);

    default long getNumberOfEntriesDelayed() {
        return 0;
    }

    List<Consumer> getConsumers();

    CompletableFuture<Void> close();

    CompletableFuture<Void> delete();

    CompletableFuture<Void> deleteForcefully();

    CompletableFuture<Void> disconnect();

    CompletableFuture<Void> doUnsubscribe(Consumer consumer);

    /**
     * @deprecated Use {@link #purgeBacklog()} instead.
     */
    @Deprecated
    CompletableFuture<Void> clearBacklog();

    default CompletableFuture<Long> purgeBacklog() {
        return clearBacklog().thenApply(v -> 0L);
    }

    CompletableFuture<Void> skipMessages(int numMessagesToSkip);

    /**
     * @deprecated Use {@link #resetCursorTo(long)} instead.
     */
    @Deprecated
    CompletableFuture<Void> resetCursor(long timestamp);

    default CompletableFuture<Position> resetCursorTo(long timestamp) {
        return resetCursor(timestamp).thenApply(v -> null);
    }

    /**
     * @deprecated Use {@link #resetCursorTo(Position)} instead.
     */
    @Deprecated
    CompletableFuture<Void> resetCursor(Position position);

    default CompletableFuture<Position> resetCursorTo(Position position) {
        return resetCursor(position).thenApply(v -> null);
    }

    CompletableFuture<Entry> peekNthMessage(int messagePosition);

    boolean expireMessages(int messageTTLInSeconds);

    boolean expireMessages(Position position);

    void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch);

    void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions);

    void markTopicWithBatchMessagePublished();

    double getExpiredMessageRate();

    SubType getType();

    String getTypeString();

    void addUnAckedMessages(int unAckMessages);

    Map<String, String> getSubscriptionProperties();

    CompletableFuture<Void> updateSubscriptionProperties(Map<String, String> subscriptionProperties);

    default void processReplicatedSubscriptionSnapshot(ReplicatedSubscriptionsSnapshot snapshot) {
        // Default is no-op
    }

    CompletableFuture<Void> endTxn(long txnidMostBits, long txnidLeastBits, int txnAction, long lowWaterMark);

    CompletableFuture<AnalyzeBacklogResult> analyzeBacklog(Optional<Position> position);

    default int getNumberOfSameAddressConsumers(final String clientAddress) {
        int count = 0;
        if (clientAddress != null) {
            for (Consumer consumer : getConsumers()) {
                if (clientAddress.equals(consumer.getClientAddress())) {
                    count++;
                }
            }
        }
        return count;
    }

    // Subscription utils
    static boolean isCumulativeAckMode(SubType subType) {
        return SubType.Exclusive.equals(subType) || SubType.Failover.equals(subType);
    }

    static boolean isIndividualAckMode(SubType subType) {
        return SubType.Shared.equals(subType) || SubType.Key_Shared.equals(subType);
    }
}

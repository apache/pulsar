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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;

public interface Dispatcher {
    void addConsumer(Consumer consumer) throws BrokerServiceException;

    void removeConsumer(Consumer consumer) throws BrokerServiceException;

    /**
     * Indicates that this consumer is now ready to receive more messages.
     *
     * @param consumer
     */
    void consumerFlow(Consumer consumer, int additionalNumberOfMessages);

    boolean isConsumerConnected();

    List<Consumer> getConsumers();

    boolean canUnsubscribe(Consumer consumer);

    /**
     * mark dispatcher closed to stop new incoming requests and disconnect all consumers.
     *
     * @return
     */
    CompletableFuture<Void> close();

    boolean isClosed();

    /**
     * Disconnect active consumers.
     */
    CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor);

    /**
     * disconnect all consumers.
     *
     * @return
     */
    CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor);

    default CompletableFuture<Void> disconnectAllConsumers() {
        return disconnectAllConsumers(false);
    }

    void resetCloseFuture();

    /**
     * mark dispatcher open to serve new incoming requests.
     */
    void reset();

    SubType getType();

    void redeliverUnacknowledgedMessages(Consumer consumer);

    void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions);

    void addUnAckedMessages(int unAckMessages);

    RedeliveryTracker getRedeliveryTracker();

    default Optional<DispatchRateLimiter> getRateLimiter() {
        return Optional.empty();
    }

    default void updateRateLimiter(DispatchRate dispatchRate) {

    }

    default void initializeDispatchRateLimiterIfNeeded(Optional<Policies> policies) {
        //No-op
    }

    /**
     * Check with dispatcher if the message should be added to the delayed delivery tracker.
     * Return true if the message should be delayed and ignored at this point.
     */
    default boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
        return false;
    }

    default long getNumberOfDelayedMessages() {
        return 0;
    }

    default void clearDelayedMessages() {
        //No-op
    }

    default void cursorIsReset() {
        //No-op
    }

    default void markDeletePositionMoveForward() {
        // No-op
    }

    /**
     * Checks if dispatcher is stuck and unblocks the dispatch if needed.
     */
    default boolean checkAndUnblockIfStuck() {
        return false;
    }

}

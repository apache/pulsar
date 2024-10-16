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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageMetadata;

public interface Dispatcher {
    CompletableFuture<Void> addConsumer(Consumer consumer);

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
    default CompletableFuture<Void> close() {
        return close(true, Optional.empty());
    }

    CompletableFuture<Void> close(boolean disconnectClients, Optional<BrokerLookupData> assignedBrokerLookupData);

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
    default CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
        return disconnectAllConsumers(isResetCursor, Optional.empty());
    }

    default CompletableFuture<Void> disconnectAllConsumers() {
        return disconnectAllConsumers(false);
    }

    CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor,
                                                   Optional<BrokerLookupData> assignedBrokerLookupData);

    void resetCloseFuture();

    /**
     * mark dispatcher open to serve new incoming requests.
     */
    void reset();

    SubType getType();

    void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch);

    void redeliverUnacknowledgedMessages(Consumer consumer, List<Position> positions);

    void addUnAckedMessages(int unAckMessages);

    RedeliveryTracker getRedeliveryTracker();

    default Optional<DispatchRateLimiter> getRateLimiter() {
        return Optional.empty();
    }

    default void updateRateLimiter() {
        initializeDispatchRateLimiterIfNeeded();
        getRateLimiter().ifPresent(DispatchRateLimiter::updateDispatchRate);
    }

    default boolean initializeDispatchRateLimiterIfNeeded() {
        return false;
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

    default CompletableFuture<Void> clearDelayedMessages() {
        return CompletableFuture.completedFuture(null);
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

    /**
     * A callback hook after acknowledge messages.
     * @param exOfDeletion the ex of {@link org.apache.bookkeeper.mledger.ManagedCursor#asyncDelete},
     *              {@link ManagedCursor#asyncClearBacklog} or {@link ManagedCursor#asyncSkipEntries)}.
     * @param ctxOfDeletion the param ctx of calling {@link org.apache.bookkeeper.mledger.ManagedCursor#asyncDelete},
     *              {@link ManagedCursor#asyncClearBacklog} or {@link ManagedCursor#asyncSkipEntries)}.
     */
    default void afterAckMessages(Throwable exOfDeletion, Object ctxOfDeletion){}

    /**
     * Trigger a new "readMoreEntries" if the dispatching has been paused before. This method is only implemented in
     * {@link org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers} right now,
     * other implementations do not necessary implement this method.
     * @return did a resume.
     */
    default boolean checkAndResumeIfPaused(){
        return false;
    }

    default long getFilterProcessedMsgCount() {
        return 0;
    }

    default long getFilterAcceptedMsgCount() {
        return 0;
    }

    default long getFilterRejectedMsgCount() {
        return 0;
    }

    default long getFilterRescheduledMsgCount() {
        return 0;
    }

}

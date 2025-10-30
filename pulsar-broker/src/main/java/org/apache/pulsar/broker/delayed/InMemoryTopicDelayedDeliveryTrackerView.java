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
package org.apache.pulsar.broker.delayed;

import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;

/**
 * View object for a subscription that implements DelayedDeliveryTracker interface.
 * This view forwards all operations to the topic-level manager while maintaining
 * compatibility with existing dispatcher logic.
 */
@Slf4j
public class InMemoryTopicDelayedDeliveryTrackerView implements DelayedDeliveryTracker {

    private final InMemoryTopicDelayedDeliveryTrackerManager manager;
    private final InMemoryTopicDelayedDeliveryTrackerManager.SubContext subContext;
    private boolean closed = false;

    public InMemoryTopicDelayedDeliveryTrackerView(InMemoryTopicDelayedDeliveryTrackerManager manager,
                                                   InMemoryTopicDelayedDeliveryTrackerManager.SubContext subContext) {
        this.manager = manager;
        this.subContext = subContext;
    }

    @Override
    public boolean addMessage(long ledgerId, long entryId, long deliveryAt) {
        checkClosed();
        return manager.addMessageForSub(subContext, ledgerId, entryId, deliveryAt);
    }

    @Override
    public boolean hasMessageAvailable() {
        checkClosed();
        return manager.hasMessageAvailableForSub(subContext);
    }

    @Override
    public long getNumberOfDelayedMessages() {
        checkClosed();
        // Return an estimate of visible delayed messages for this subscription
        // For now, return the total count - could be enhanced to count only visible messages
        return manager.topicDelayedMessages();
    }

    @Override
    public long getBufferMemoryUsage() {
        checkClosed();
        // Return the topic-level memory usage (shared by all subscriptions)
        return manager.topicBufferMemoryBytes();
    }

    @Override
    public NavigableSet<Position> getScheduledMessages(int maxMessages) {
        checkClosed();
        return manager.getScheduledMessagesForSub(subContext, maxMessages);
    }

    @Override
    public boolean shouldPauseAllDeliveries() {
        checkClosed();
        return manager.shouldPauseAllDeliveriesForSub(subContext);
    }

    @Override
    public void resetTickTime(long tickTime) {
        checkClosed();
        manager.onTickTimeUpdated(tickTime);
    }

    @Override
    public CompletableFuture<Void> clear() {
        checkClosed();
        // For topic-level manager, clear is a no-op for individual subscriptions
        manager.clearForSub();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        manager.unregister(subContext.getDispatcher());
    }

    /**
     * Update the mark delete position for this subscription.
     * This is called by the dispatcher when messages are acknowledged.
     */
    public void updateMarkDeletePosition(Position position) {
        checkClosed();
        manager.updateMarkDeletePosition(subContext, position);
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("DelayedDeliveryTracker is already closed");
        }
    }
}
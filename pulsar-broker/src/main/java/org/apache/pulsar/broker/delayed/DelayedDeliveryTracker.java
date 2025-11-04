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

import com.google.common.annotations.Beta;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;

/**
 * Represent the tracker for the delayed delivery of messages for a particular subscription.
 *
 * Note: this interface is still being refined and some breaking changes might be introduced.
 */
@Beta
public interface DelayedDeliveryTracker extends AutoCloseable {

    /**
     * Add a message to the tracker.
     *
     * @param ledgerId   the ledgerId
     * @param entryId    the entryId
     * @param deliveryAt the absolute timestamp at which the message should be tracked
     * @return true if the message was added to the tracker or false if it should be delivered immediately
     */
    boolean addMessage(long ledgerId, long entryId, long deliveryAt);

    /**
     * Return true if there's at least a message that is scheduled to be delivered already.
     */
    boolean hasMessageAvailable();

    /**
     * @return the number of delayed messages being tracked.
     */
    long getNumberOfDelayedMessages();

    /**
     * The amount of memory used to back the delayed message index.
     */
    long getBufferMemoryUsage();

    /**
     * Get a set of position of messages that have already reached the delivery time.
     */
    NavigableSet<Position> getScheduledMessages(int maxMessages);

    /**
     * Tells whether the dispatcher should pause any message deliveries, until the DelayedDeliveryTracker has
     * more messages available.
     */
    boolean shouldPauseAllDeliveries();

    /**
     *  Reset tick time use zk policies cache.
     * @param tickTime
     *          The tick time for when retrying on delayed delivery messages
     */
    void resetTickTime(long tickTime);

    /**
     * Clear all delayed messages from the tracker.
     *
     * @return CompletableFuture<Void>
     */
    CompletableFuture<Void> clear();

    /**
     * Close the subscription tracker and release all resources.
     */
    void close();

    DelayedDeliveryTracker DISABLE = new DelayedDeliveryTracker() {
        @Override
        public boolean addMessage(long ledgerId, long entryId, long deliveryAt) {
            return false;
        }

        @Override
        public boolean hasMessageAvailable() {
            return false;
        }

        @Override
        public long getNumberOfDelayedMessages() {
            return 0;
        }

        @Override
        public long getBufferMemoryUsage() {
            return 0;
        }

        @Override
        public NavigableSet<Position> getScheduledMessages(int maxMessages) {
            return null;
        }

        @Override
        public boolean shouldPauseAllDeliveries() {
            return false;
        }

        @Override
        public void resetTickTime(long tickTime) {

        }

        @Override
        public CompletableFuture<Void> clear() {
            return null;
        }

        @Override
        public void close() {

        }
    };
}

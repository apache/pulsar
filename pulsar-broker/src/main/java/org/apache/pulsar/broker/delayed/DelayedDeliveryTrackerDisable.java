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

import org.apache.bookkeeper.mledger.impl.PositionImpl;

import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;

class DelayedDeliveryTrackerDisable implements DelayedDeliveryTracker {

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
    public NavigableSet<PositionImpl> getScheduledMessages(int maxMessages) {
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
}

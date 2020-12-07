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
package org.apache.pulsar.broker.service.dispatcher;

import lombok.NoArgsConstructor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.common.api.proto.PulsarApi;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@NoArgsConstructor
public class MockCustomizedDispatcher implements Dispatcher{
    @Override
    public void addConsumer(Consumer consumer) throws BrokerServiceException { }

    @Override
    public void removeConsumer(Consumer consumer) throws BrokerServiceException { }

    @Override
    public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) { }

    @Override
    public boolean isConsumerConnected() { return false; }

    @Override
    public List<Consumer> getConsumers() { return null; }

    @Override
    public boolean canUnsubscribe(Consumer consumer) { return false; }

    @Override
    public CompletableFuture<Void> close() { return null; }

    @Override
    public boolean isClosed() { return false; }

    @Override
    public CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) { return null; }

    @Override
    public CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) { return null; }

    @Override
    public void resetCloseFuture() { }

    @Override
    public void reset() { }

    @Override
    public PulsarApi.CommandSubscribe.SubType getType() { return null; }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer) { }

    @Override
    public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) { }

    @Override
    public void addUnAckedMessages(int unAckMessages) { }

    @Override
    public RedeliveryTracker getRedeliveryTracker() { return null; }
}

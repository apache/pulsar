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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Routing {@link TransactionPendingAckStoreProvider}: returns {@link MetadataPendingAckStore} for
 * subscriptions on {@code segment://} topics (PIP-473) and falls back to {@link MLPendingAckStore}
 * via {@link MLPendingAckStoreProvider} for {@code persistent://} / {@code topic://}.
 *
 * <p>Available but not the configured default — operators (and P5.4) flip
 * {@code transactionPendingAckStoreProviderClassName} to opt segment topics into the
 * metadata-driven implementation once the v5 TC is in place.
 */
public class DispatchingTransactionPendingAckStoreProvider implements TransactionPendingAckStoreProvider {

    private final TransactionPendingAckStoreProvider legacy = new MLPendingAckStoreProvider();
    private final TransactionPendingAckStoreProvider metadata = new MetadataPendingAckStoreProvider();

    @Override
    public CompletableFuture<PendingAckStore> newPendingAckStore(PersistentSubscription subscription) {
        return delegateFor(subscription).newPendingAckStore(subscription);
    }

    @Override
    public CompletableFuture<Boolean> checkInitializedBefore(PersistentSubscription subscription) {
        return delegateFor(subscription).checkInitializedBefore(subscription);
    }

    private TransactionPendingAckStoreProvider delegateFor(PersistentSubscription subscription) {
        return TopicName.get(subscription.getTopicName()).isSegment() ? metadata : legacy;
    }
}

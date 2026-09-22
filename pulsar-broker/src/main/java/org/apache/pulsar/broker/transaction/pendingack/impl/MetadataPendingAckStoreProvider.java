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
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;

/**
 * Provider that builds a {@link MetadataPendingAckStore} backed by the broker's local
 * {@code MetadataStore}. Intended for {@code segment://} subscriptions. The dispatching provider
 * routes here when the topic is a segment topic.
 */
public class MetadataPendingAckStoreProvider implements TransactionPendingAckStoreProvider {

    @Override
    public CompletableFuture<PendingAckStore> newPendingAckStore(PersistentSubscription subscription) {
        TxnMetadataStore txnStore = new TxnMetadataStore(
                subscription.getTopic().getBrokerService().getPulsar().getLocalMetadataStore());
        return CompletableFuture.completedFuture(new MetadataPendingAckStore(subscription, txnStore));
    }

    @Override
    public CompletableFuture<Boolean> checkInitializedBefore(PersistentSubscription subscription) {
        // The metadata layout is global — there is no per-subscription "initialized" log to
        // check. State (open txns, leftover op records) is rebuilt on demand from the
        // /txn/op + /txn records at replay time.
        return CompletableFuture.completedFuture(true);
    }
}

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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckStore;
import org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider;
import org.apache.pulsar.broker.transaction.pendingack.exceptions.TransactionPendingAckStoreProviderException;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Provider is for MLPendingAckStore.
 */
@Slf4j
public class MLPendingAckStoreProvider implements TransactionPendingAckStoreProvider {

    @Override
    public CompletableFuture<PendingAckStore> newPendingAckStore(Topic originTopic, String subName) {
        CompletableFuture<PendingAckStore> pendingAckStoreFuture = new CompletableFuture<>();

        if (originTopic == null) {
            pendingAckStoreFuture.completeExceptionally(
                    new TransactionPendingAckStoreProviderException("The originTopic is null."));
            return pendingAckStoreFuture;
        }
        if (!(originTopic instanceof PersistentTopic)) {
            pendingAckStoreFuture.completeExceptionally(new TransactionPendingAckStoreProviderException(
                    "The originTopic is not persistentTopic."));
            return pendingAckStoreFuture;
        }

        PersistentTopic originPersistentTopic = (PersistentTopic) originTopic;
        String pendingAckTopicName = MLPendingAckStore
                .getTransactionPendingAckStoreSuffix(originPersistentTopic.getName(), subName);

        originPersistentTopic.getBrokerService().getManagedLedgerFactory()
                .asyncOpen(TopicName.get(pendingAckTopicName).getPersistenceNamingEncoding(),
                        new AsyncCallbacks.OpenLedgerCallback() {
                            @Override
                            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                                ledger.asyncOpenCursor(MLPendingAckStore.getTransactionPendingAckStoreCursorName(),
                                        InitialPosition.Earliest, new AsyncCallbacks.OpenCursorCallback() {
                                    @Override
                                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                                        pendingAckStoreFuture.complete(new MLPendingAckStore(ledger, cursor));
                                    }

                                    @Override
                                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                                        log.error("Open MLPendingAckStore cursor failed.", exception);
                                        pendingAckStoreFuture.completeExceptionally(exception);
                                    }
                                }, null);
                            }

                            @Override
                            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                                log.error("Open MLPendingAckStore managedLedger failed.", exception);
                                pendingAckStoreFuture.completeExceptionally(exception);
                            }
                        }, null);
        return pendingAckStoreFuture;
    }
}

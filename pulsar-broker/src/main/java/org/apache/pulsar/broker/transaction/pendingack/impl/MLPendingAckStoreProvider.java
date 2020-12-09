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
import java.util.concurrent.TimeUnit;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
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

    private volatile Timer timer;

    private static final long tickTimeMillis = 1L;

    @Override
    public CompletableFuture<PendingAckStore> newPendingAckStore(PersistentSubscription subscription, String subName) {
        CompletableFuture<PendingAckStore> pendingAckStoreFuture = new CompletableFuture<>();

        if (subscription == null) {
            pendingAckStoreFuture.completeExceptionally(
                    new TransactionPendingAckStoreProviderException("The subscription is null."));
            return pendingAckStoreFuture;
        }

        PersistentTopic originPersistentTopic = (PersistentTopic) subscription.getTopic();
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
                                                if (timer == null) {
                                                    synchronized (MLPendingAckStoreProvider.this) {
                                                        if (timer == null) {
                                                            initialize();
                                                        }
                                                    }
                                                }
                                                int maxIntervalTime =
                                                        originPersistentTopic.getBrokerService().getPulsar()
                                                                .getConfig().getMaxPendingAckTimerTaskIntervalTime();
                                                int minIntervalTime =
                                                        originPersistentTopic.getBrokerService().getPulsar()
                                                                .getConfig().getMaxPendingAckTimerTaskIntervalTime();
                                                pendingAckStoreFuture
                                                        .complete(new MLPendingAckStore(ledger, cursor, timer,
                                                                subscription.getCursor(),
                                                                maxIntervalTime, minIntervalTime));
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

    public void initialize() {
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("pending-ack-provider-timer"),
                tickTimeMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (timer != null) {
            timer.stop();
        }
    }
}
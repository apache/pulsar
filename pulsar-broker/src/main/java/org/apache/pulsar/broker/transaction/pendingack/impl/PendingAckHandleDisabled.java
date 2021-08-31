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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.pulsar.broker.service.BrokerServiceException.NotAllowedException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.transaction.pendingack.PendingAckHandle;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.policies.data.TransactionInPendingAckStats;
import org.apache.pulsar.common.policies.data.TransactionPendingAckStats;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * The disabled implementation of {@link PendingAckHandle}.
 */
public class PendingAckHandleDisabled implements PendingAckHandle {

    private final CompletableFuture<PendingAckHandle> pendingAckHandleCompletableFuture =
            CompletableFuture.completedFuture(PendingAckHandleDisabled.this);

    @Override
    public CompletableFuture<Void> individualAcknowledgeMessage(TxnID txnID,
                                                                List<MutablePair<PositionImpl, Integer>> positions,
                                                                boolean isInCacheRequest) {
        return FutureUtil.failedFuture(new NotAllowedException("The transaction is disabled"));
    }

    @Override
    public CompletableFuture<Void> cumulativeAcknowledgeMessage(TxnID txnID, List<PositionImpl> positions,
                                                                boolean isInCacheRequest) {
        return FutureUtil.failedFuture(new NotAllowedException("The transaction is disabled"));
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, Map<String, Long> properties, long lowWaterMark,
                                             boolean isInCacheRequest) {
        return FutureUtil.failedFuture(new NotAllowedException("The transaction is disabled"));
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnId, Consumer consumer, long lowWaterMark,
                                            boolean isInCacheRequest) {
        return FutureUtil.failedFuture(new NotAllowedException("The transaction is disabled"));
    }

    @Override
    public void syncBatchPositionAckSetForTransaction(PositionImpl position) {
        //no operation
    }

    @Override
    public boolean checkIsCanDeleteConsumerPendingAck(PositionImpl position) {
        return false;
    }

    @Override
    public void clearIndividualPosition(Position position) {
        //no-op
    }

    @Override
    public CompletableFuture<PendingAckHandle> pendingAckHandleFuture() {
        return pendingAckHandleCompletableFuture;
    }

    @Override
    public TransactionInPendingAckStats getTransactionInPendingAckStats(TxnID txnID) {
        return null;
    }

    @Override
    public TransactionPendingAckStats getStats() {
        return null;
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.completedFuture(null);
    }
}

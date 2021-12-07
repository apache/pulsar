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
package org.apache.pulsar.broker.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Transaction buffer disable.
 */
@Slf4j
public class TransactionBufferDisable implements TransactionBuffer {

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return null;
    }

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException("Transaction buffer disable!"));
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException("Transaction buffer disable!"));
    }
    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long lowWaterMark) {
        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException("Transaction buffer disable!"));
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, long lowWaterMark) {
        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException("Transaction buffer disable!"));
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        return null;
    }

    @Override
    public CompletableFuture<Void> clearSnapshot() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isTxnAborted(TxnID txnID) {
        return false;
    }

    @Override
    public void syncMaxReadPositionForNormalPublish(PositionImpl position) {
        //no-op
    }

    @Override
    public PositionImpl getMaxReadPosition() {
        return PositionImpl.latest;
    }

    @Override
    public TransactionInBufferStats getTransactionInBufferStats(TxnID txnID) {
        return null;
    }

    @Override
    public TransactionBufferStats getStats() {
        return null;
    }

    @Override
    public CompletableFuture<Void> checkIfTBRecoverCompletely(boolean isTxn) {
        return CompletableFuture.completedFuture(null);
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.transaction.buffer.TransactionCursor;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;

public class PersistentTransactionTopic extends PersistentTopic implements TransactionBuffer {

    static final String TRANSACTION_CURSOR_NAME = "pulsar.transaction";

    private TransactionCursor transactionCursor;

    public PersistentTransactionTopic(String topic, ManagedLedger ledger, BrokerService brokerService)
        throws BrokerServiceException.NamingException {
        super(topic, ledger, brokerService);
        this.transactionCursor = new TransactionCursorImpl(ledger, brokerService.pulsar());
    }

    @Override
    public void publishMessage(ByteBuf headersAndPayload, PublishContext publishContext) {
        ByteBuf data = headersAndPayload.copy();
        super.publishMessage(headersAndPayload, publishContext);
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        CompletableFuture<TransactionMeta> getFuture = new CompletableFuture<>();
        try {
            getFuture.complete(transactionCursor.getTxnMeta(txnID));
        } catch (TransactionNotFoundException e) {
            getFuture.completeExceptionally(e);
        }
        return getFuture;
    }


    @Override
    public CompletableFuture<Void> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        CompletableFuture future = new CompletableFuture();


        return future;
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long committedAtLedgerId, long committedAtEntryId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID) {
        return null;
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        return null;
    }

    @Override
    public void close() {

    }

}

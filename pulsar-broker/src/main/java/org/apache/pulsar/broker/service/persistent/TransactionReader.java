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
package org.apache.pulsar.broker.service.persistent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.AbstractBaseDispatcher;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * Used to read transaction messages for dispatcher.
 */
@Slf4j
public class TransactionReader {

    private final AbstractBaseDispatcher dispatcher;
    private volatile TransactionBuffer transactionBuffer;
    private volatile long startSequenceId = 0;
    private volatile CompletableFuture<TransactionBufferReader> transactionBufferReader;

    public TransactionReader(AbstractBaseDispatcher abstractBaseDispatcher) {
        this.dispatcher = abstractBaseDispatcher;
    }

    /**
     * Get ${@link TransactionBuffer} lazily and read transaction messages.
     *
     * @param readMessageNum messages num to read
     * @param ctx context object
     * @param readEntriesCallback ReadEntriesCallback
     */
    public void read(int readMessageNum, Object ctx, AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        if (transactionBuffer == null) {
            dispatcher.getSubscription().getTopic()
                    .getTransactionBuffer(false).whenComplete((tb, throwable) -> {
                if (throwable != null) {
                    log.error("Get transactionBuffer failed.", throwable);
                    readEntriesCallback.readEntriesFailed(
                            ManagedLedgerException.getManagedLedgerException(throwable), ctx);
                    return;
                }
                transactionBuffer = tb;
                internalRead(readMessageNum, ctx, readEntriesCallback);
            });
        } else {
            internalRead(readMessageNum, ctx, readEntriesCallback);
        }
    }

    /**
     * Read specify number transaction messages by ${@link TransactionBufferReader}.
     *
     * @param readMessageNum messages num to read
     * @param ctx context object
     * @param readEntriesCallback ReadEntriesCallback
     */
    private void internalRead(int readMessageNum, Object ctx, AsyncCallbacks.ReadEntriesCallback readEntriesCallback) {
        final TxnID txnID = getValidTxn();
        if (txnID == null) {
            log.error("No valid txn to read.");
            readEntriesCallback.readEntriesFailed(
                    ManagedLedgerException.getManagedLedgerException(new Exception("No valid txn to read.")), ctx);
            return;
        }
        if (transactionBufferReader == null) {
            transactionBufferReader = transactionBuffer.openTransactionBufferReader(txnID, startSequenceId);
        }
        transactionBufferReader.thenAccept(reader -> {
            reader.readNext(readMessageNum).whenComplete((transactionEntries, throwable) -> {
                if (throwable != null) {
                    log.error("Read transaction messages failed.", throwable);
                    readEntriesCallback.readEntriesFailed(
                            ManagedLedgerException.getManagedLedgerException(throwable), ctx);
                    return;
                }
                if (transactionEntries == null || transactionEntries.size() < readMessageNum) {
                    startSequenceId = 0;
                    dispatcher.getPendingTxnQueue().remove(txnID);
                    transactionBufferReader = null;
                    reader.close();
                }
                List<Entry> entryList = new ArrayList<>(transactionEntries.size());
                for (int i = 0; i < transactionEntries.size(); i++) {
                    if (i == (transactionEntries.size() -1)) {
                        startSequenceId = transactionEntries.get(i).sequenceId();
                    }
                    entryList.add(transactionEntries.get(i).getEntry());
                }
                readEntriesCallback.readEntriesComplete(entryList, ctx);
            });
        }).exceptionally(throwable -> {
            log.error("Open transactionBufferReader failed.", throwable);
            readEntriesCallback.readEntriesFailed(
                    ManagedLedgerException.getManagedLedgerException(throwable), ctx);
            return null;
        });
    }

    private TxnID getValidTxn() {
        TxnID txnID;
        do {
            txnID = dispatcher.getPendingTxnQueue().peek();
            if (txnID == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Peek null txnID from dispatcher pendingTxnQueue.");
                }
                dispatcher.getPendingTxnQueue().poll();
                if (dispatcher.getPendingTxnQueue().size() <= 0) {
                    break;
                }
            }
        } while (txnID == null);
        return txnID;
    }

}

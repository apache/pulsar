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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.Queues;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionEntry;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * Used to read transaction messages for dispatcher.
 * This TransactionReader read only one transaction data one time, and read transaction saved in the queue one by one.
 */
@Slf4j
public class TransactionReader {

    private final Topic topic;
    private final ManagedCursor managedCursor;
    private final ConcurrentLinkedQueue<TxnID> pendingTxnQueue;

    private TransactionBuffer transactionBuffer;
    private CompletableFuture<TransactionBufferReader> transactionBufferReader;
    private int startBatchIndex = 0;

    public TransactionReader(Topic topic, ManagedCursor managedCursor) {
        this.topic = topic;
        this.managedCursor = managedCursor;
        this.pendingTxnQueue = Queues.newConcurrentLinkedQueue();
    }

    public void addPendingTxn(long txnidMostBits, long txnidLatestBits) {
        pendingTxnQueue.add(new TxnID(txnidMostBits, txnidLatestBits));
    }

    public boolean havePendingTxnToRead() {
        return pendingTxnQueue.size() > 0;
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
            topic.getTransactionBuffer(false).whenComplete((tb, throwable) -> {
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
            transactionBufferReader = transactionBuffer.openTransactionBufferReader(txnID, -1);
        }
        transactionBufferReader.thenAccept(reader -> {
            reader.readNext(readMessageNum).whenComplete((transactionEntries, throwable) -> {
                if (throwable != null) {
                    log.error("Read transaction messages failed.", throwable);
                    readEntriesCallback.readEntriesFailed(
                            ManagedLedgerException.getManagedLedgerException(throwable), ctx);
                    return;
                }

                if (transactionEntries == null || transactionEntries.size() == 0) {
                    resetReader(txnID, reader);
                    readEntriesCallback.readEntriesComplete(Collections.EMPTY_LIST, ctx);
                    return;
                }

                ((ManagedCursorImpl) managedCursor).internalInitBatchDeletedIndex(
                        PositionImpl.get(
                                transactionEntries.get(0).committedAtLedgerId(),
                                transactionEntries.get(0).committedAtEntryId()),
                        transactionEntries.get(0).numMessageInTxn());

                if (transactionEntries.size() < readMessageNum) {
                    resetReader(txnID, reader);
                }
                readEntriesCallback.readEntriesComplete(new ArrayList<>(transactionEntries), ctx);
            });
        }).exceptionally(throwable -> {
            log.error("Open transactionBufferReader failed.", throwable);
            readEntriesCallback.readEntriesFailed(
                    ManagedLedgerException.getManagedLedgerException(throwable), ctx);
            return null;
        });
    }

    private void resetReader(TxnID txnID, TransactionBufferReader reader) {
        startBatchIndex = 0;
        pendingTxnQueue.remove(txnID);
        transactionBufferReader = null;
        reader.close();
    }

    private TxnID getValidTxn() {
        TxnID txnID;
        do {
            txnID = pendingTxnQueue.peek();
            if (txnID == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Peek null txnID from dispatcher pendingTxnQueue.");
                }
                pendingTxnQueue.poll();
                if (pendingTxnQueue.size() <= 0) {
                    break;
                }
            }
        } while (txnID == null);
        return txnID;
    }

    /**
     * Calculate the startBatchIndex for the Entry,
     * the batchIndex accumulate the numMessagesInBatch,
     * when reading one transaction finished the startBatchIndex will be reset to 0.
     *
     * @param numMessagesInBatch the number messages in a batch
     * @return startBatchIndex of the Entry
     */
    public int calculateStartBatchIndex(int numMessagesInBatch) {
        int startBatchIndex = this.startBatchIndex;
        this.startBatchIndex += numMessagesInBatch;
        return startBatchIndex;
    }

}

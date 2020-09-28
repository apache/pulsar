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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.buffer.exceptions.EndOfTransactionException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionSealedException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionStatusException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

public class TransactionMetaImpl implements TransactionMeta {

    private final TxnID txnID;
    private SortedMap<Long, Position> entries;
    private TxnStatus txnStatus;
    private long committedAtLedgerId = -1L;
    private long committedAtEntryId = -1L;
    private int numMessageInTxn;

    TransactionMetaImpl(TxnID txnID) {
        this.txnID = txnID;
        this.entries = new TreeMap<>();
        this.txnStatus = TxnStatus.OPEN;
    }

    @Override
    public TxnID id() {
        return this.txnID;
    }

    @Override
    public synchronized TxnStatus status() {
        return this.txnStatus;
    }

    @Override
    public int numEntries() {
        synchronized (entries) {
            return entries.size();
        }
    }

    @Override
    public int numMessageInTxn() throws TransactionStatusException {
        if (!checkStatus(TxnStatus.COMMITTING, null) && !checkStatus(TxnStatus.COMMITTED, null)) {
            throw new TransactionStatusException(txnID, TxnStatus.COMMITTED, txnStatus);
        }
        return numMessageInTxn;
    }

    @VisibleForTesting
    public SortedMap<Long, Position> getEntries() {
        return entries;
    }

    @Override
    public long committedAtLedgerId() {
        return committedAtLedgerId;
    }

    @Override
    public long committedAtEntryId() {
        return committedAtEntryId;
    }

    @Override
    public long lastSequenceId() {
        return entries.lastKey();
    }

    @Override
    public CompletableFuture<SortedMap<Long, Position>> readEntries(int num, long startSequenceId) {
        CompletableFuture<SortedMap<Long, Position>> readFuture = new CompletableFuture<>();

        SortedMap<Long, Position> result = new TreeMap<>();

        SortedMap<Long, Position> readEntries = entries;
        if (startSequenceId != PersistentTransactionBufferReader.DEFAULT_START_SEQUENCE_ID) {
            readEntries = entries.tailMap(startSequenceId + 1);
        }

        if (readEntries.isEmpty()) {
            readFuture.completeExceptionally(
                new EndOfTransactionException("No more entries found in transaction `" + txnID + "`"));
            return readFuture;
        }

        for (Map.Entry<Long, Position> longPositionEntry : readEntries.entrySet()) {
            result.put(longPositionEntry.getKey(), longPositionEntry.getValue());
        }

        readFuture.complete(result);

        return readFuture;
    }

    @Override
    public CompletableFuture<Position> appendEntry(long sequenceId, Position position, int batchSize) {
        CompletableFuture<Position> appendFuture = new CompletableFuture<>();
        synchronized (this) {
            if (TxnStatus.OPEN != txnStatus) {
                appendFuture.completeExceptionally(
                    new TransactionSealedException("Transaction `" + txnID + "` is " + "already sealed"));
                return appendFuture;
            }
        }
        synchronized (this.entries) {
            this.entries.put(sequenceId, position);
            this.numMessageInTxn += batchSize;
        }
        return CompletableFuture.completedFuture(position);
    }

    @Override
    public CompletableFuture<TransactionMeta> committingTxn() {
        CompletableFuture<TransactionMeta> committingFuture = new CompletableFuture<>();
        if (!checkStatus(TxnStatus.OPEN, committingFuture)) {
            return committingFuture;
        }
        this.txnStatus = TxnStatus.COMMITTING;
        committingFuture.complete(this);
        return committingFuture;
    }

    @Override
    public synchronized CompletableFuture<TransactionMeta> commitTxn(long committedAtLedgerId,
                                                                     long committedAtEntryId) {
        CompletableFuture<TransactionMeta> commitFuture = new CompletableFuture<>();
        if (!checkStatus(TxnStatus.COMMITTING, commitFuture)) {
            return commitFuture;
        }

        this.committedAtLedgerId = committedAtLedgerId;
        this.committedAtEntryId = committedAtEntryId;
        this.txnStatus = TxnStatus.COMMITTED;
        TransactionMeta meta = this;
        commitFuture.complete(meta);
        return commitFuture;
    }

    @Override
    public synchronized CompletableFuture<TransactionMeta> abortTxn() {
        CompletableFuture<TransactionMeta> abortFuture = new CompletableFuture<>();
        if (!checkStatus(TxnStatus.OPEN, abortFuture)) {
            return abortFuture;
        }

        this.txnStatus = TxnStatus.ABORTED;
        abortFuture.complete(this);

        return abortFuture;
    }

    private boolean checkStatus(TxnStatus expectedStatus, CompletableFuture<TransactionMeta> future) {
        if (!txnStatus.equals(expectedStatus)) {
            if (future != null) {
                future.completeExceptionally(new TransactionStatusException(txnID, expectedStatus, txnStatus));
            }
            return false;
        }
        return true;
    }

}

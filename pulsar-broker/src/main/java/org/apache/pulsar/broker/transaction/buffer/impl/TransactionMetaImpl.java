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
import org.apache.pulsar.broker.transaction.buffer.exceptions.UnexpectedTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

public class TransactionMetaImpl implements TransactionMeta {

    private final TxnID txnID;
    private SortedMap<Long, Position> entries;
    private TxnStatus txnStatus;
    private long committedAtLedgerId = -1L;
    private long committedAtEntryId = -1L;

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
            readEntries = entries.tailMap(startSequenceId);
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
    public CompletableFuture<Void> appendEntry(long sequenceId, Position position) {
        CompletableFuture<Void> appendFuture = new CompletableFuture<>();
        synchronized (this) {
            if (TxnStatus.OPEN != txnStatus) {
                appendFuture.completeExceptionally(
                    new TransactionSealedException("Transaction `" + txnID + "` is " + "already sealed"));
                return appendFuture;
            }
        }
        synchronized (this.entries) {
            this.entries.put(sequenceId, position);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<TransactionMeta> commitTxn(long committedAtLedgerId,
                                                                     long committedAtEntryId) {
        CompletableFuture<TransactionMeta> commitFuture = new CompletableFuture<>();
        if (!checkOpened(txnID, commitFuture)) {
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
        if (!checkOpened(txnID, abortFuture)) {
            return abortFuture;
        }

        this.txnStatus = TxnStatus.ABORTED;
        abortFuture.complete(this);

        return abortFuture;
    }

    private boolean checkOpened(TxnID txnID, CompletableFuture<TransactionMeta> future) {
        if (TxnStatus.OPEN != txnStatus) {
            future.completeExceptionally(new UnexpectedTxnStatusException(txnID, TxnStatus.OPEN, txnStatus));
            return false;
        }
        return true;
    }
}

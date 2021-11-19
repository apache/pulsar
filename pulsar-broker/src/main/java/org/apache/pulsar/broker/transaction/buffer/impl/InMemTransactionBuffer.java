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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.exception.buffer.TransactionBufferException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;
import org.apache.pulsar.common.policies.data.TransactionInBufferStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;

/**
 * The in-memory implementation of {@link TransactionBuffer}.
 */
class InMemTransactionBuffer implements TransactionBuffer {

    /**
     * A class represents the buffer of a transaction.
     */
    private static class TxnBuffer implements TransactionMeta, AutoCloseable {

        private final TxnID txnid;
        private final SortedMap<Long, ByteBuf> entries;
        private TxnStatus status;
        private long committedAtLedgerId = -1L;
        private long committedAtEntryId = -1L;

        TxnBuffer(TxnID txnid) {
            this.txnid = txnid;
            this.entries = new TreeMap<>();
            this.status = TxnStatus.OPEN;
        }

        @Override
        public TxnID id() {
            return txnid;
        }

        @Override
        public synchronized TxnStatus status() {
            return status;
        }

        @Override
        public int numEntries() {
            synchronized (entries) {
                return entries.size();
            }
        }

        @Override
        public int numMessageInTxn() throws TransactionBufferException.TransactionStatusException {
            return -1;
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
            return FutureUtil.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Position> appendEntry(long sequenceId, Position position, int batchSize) {
            return FutureUtil.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<TransactionMeta> committingTxn() {
            status = TxnStatus.COMMITTING;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<TransactionMeta> commitTxn(long committedAtLedgerId, long committedAtEntryId) {
            try {
                return CompletableFuture.completedFuture(commitAt(committedAtLedgerId, committedAtEntryId));
            } catch (TransactionBufferException.TransactionStatusException e) {
                return FutureUtil.failedFuture(e);
            }
        }

        @Override
        public CompletableFuture<TransactionMeta> abortTxn() {
            try {
                return CompletableFuture.completedFuture(abort());
            } catch (TransactionBufferException.TransactionStatusException e) {
                return FutureUtil.failedFuture(e);
            }
        }

        synchronized TxnBuffer abort() throws TransactionBufferException.TransactionStatusException {
            if (TxnStatus.OPEN != status) {
                throw new TransactionBufferException.TransactionStatusException(txnid, TxnStatus.OPEN, status);
            }
            this.status = TxnStatus.ABORTED;
            return this;
        }

        synchronized TxnBuffer commitAt(long committedAtLedgerId, long committedAtEntryId)
                throws TransactionBufferException.TransactionStatusException {
            if (TxnStatus.OPEN != status) {
                throw new TransactionBufferException.TransactionStatusException(txnid, TxnStatus.OPEN, status);
            }

            this.committedAtLedgerId = committedAtLedgerId;
            this.committedAtEntryId = committedAtEntryId;
            this.status = TxnStatus.COMMITTED;

            return this;
        }

        @Override
        public void close() {
            synchronized (entries) {
                entries.forEach((sequenceId, buffer) -> {
                    buffer.release();
                });
                entries.clear();
            }
        }

        public void appendEntry(long sequenceId, ByteBuf entry) throws
                TransactionBufferException.TransactionSealedException {
            synchronized (this) {
                if (TxnStatus.OPEN != status) {
                    // the transaction is not open anymore, reject the append operations
                    throw new TransactionBufferException
                            .TransactionSealedException("Transaction `" + txnid + "` is already sealed");
                }
            }

            synchronized (this.entries) {
                this.entries.put(sequenceId, entry);
            }
        }

        public TransactionBufferReader newReader(long sequenceId) throws
                TransactionBufferException.TransactionNotSealedException {
            synchronized (this) {
                if (TxnStatus.COMMITTED != status) {
                    // the transaction is not committed yet, hence the buffer is not sealed
                    throw new TransactionBufferException
                            .TransactionNotSealedException("Transaction `" + txnid + "` is not sealed yet");
                }
            }

            final SortedMap<Long, ByteBuf> entriesToRead = new TreeMap<>();
            synchronized (entries) {
                SortedMap<Long, ByteBuf> subEntries = entries.tailMap(sequenceId);
                subEntries.values().forEach(value -> value.retain());
                entriesToRead.putAll(subEntries);
            }

            return new InMemTransactionBufferReader(
                txnid,
                entriesToRead.entrySet().iterator(),
                committedAtLedgerId,
                committedAtEntryId
            );
        }

    }

    final ConcurrentMap<TxnID, TxnBuffer> buffers;
    final Map<Long, Set<TxnID>> txnIndex;
    public InMemTransactionBuffer(Topic topic) {
        this.buffers = new ConcurrentHashMap<>();
        this.txnIndex = new HashMap<>();
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        CompletableFuture<TransactionMeta> getFuture = new CompletableFuture<>();
        try {
            getFuture.complete(getTxnBufferOrThrowNotFoundException(txnID));
        } catch (TransactionBufferException.TransactionNotFoundException e) {
            getFuture.completeExceptionally(e);
        }
        return getFuture;
    }

    private TxnBuffer getTxnBufferOrThrowNotFoundException(TxnID txnID)
            throws TransactionBufferException.TransactionNotFoundException {
        TxnBuffer buffer = buffers.get(txnID);
        if (null == buffer) {
            throw new TransactionBufferException.TransactionNotFoundException(
                "Transaction `" + txnID + "` doesn't exist in the transaction buffer");
        }
        return buffer;
    }

    private TxnBuffer getTxnBufferOrCreateIfNotExist(TxnID txnID) {
        TxnBuffer buffer = buffers.get(txnID);
        if (null == buffer) {
            TxnBuffer newBuffer = new TxnBuffer(txnID);
            TxnBuffer oldBuffer = buffers.putIfAbsent(txnID, newBuffer);
            if (null != oldBuffer) {
                newBuffer.close();
                return oldBuffer;
            } else {
                return newBuffer;
            }
        } else {
            return buffer;
        }
    }

    @Override
    public CompletableFuture<Position> appendBufferToTxn(TxnID txnId,
                                                     long sequenceId,
                                                     ByteBuf buffer) {
        TxnBuffer txnBuffer = getTxnBufferOrCreateIfNotExist(txnId);

        CompletableFuture appendFuture = new CompletableFuture();
        try {
            txnBuffer.appendEntry(sequenceId, buffer);
            appendFuture.complete(null);
        } catch (TransactionBufferException.TransactionSealedException e) {
            appendFuture.completeExceptionally(e);
        }
        return appendFuture;
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(
            TxnID txnID, long startSequenceId) {
        CompletableFuture<TransactionBufferReader> openFuture = new CompletableFuture<>();
        try {
            TxnBuffer txnBuffer = getTxnBufferOrThrowNotFoundException(txnID);
            TransactionBufferReader reader = txnBuffer.newReader(startSequenceId);
            openFuture.complete(reader);
        } catch (TransactionBufferException.TransactionNotFoundException
                | TransactionBufferException.TransactionNotSealedException e) {
            openFuture.completeExceptionally(e);
        }
        return openFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long lowWaterMark) {
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();
        try {
            TxnBuffer txnBuffer = getTxnBufferOrThrowNotFoundException(txnID);
            synchronized (txnBuffer) {
                // the committed position should be generated
                long committedAtLedgerId = -1L;
                long committedAtEntryId = -1L;
                txnBuffer.commitAt(committedAtLedgerId, committedAtEntryId);
                addTxnToTxnIdex(txnID, committedAtLedgerId);
            }
            commitFuture.complete(null);
        } catch (TransactionBufferException.TransactionNotFoundException
                | TransactionBufferException.TransactionStatusException e) {
            commitFuture.completeExceptionally(e);
        }
        return commitFuture;
    }

    private void addTxnToTxnIdex(TxnID txnId, long committedAtLedgerId) {
        synchronized (txnIndex) {
            txnIndex
                .computeIfAbsent(committedAtLedgerId, ledgerId -> new HashSet<>())
                .add(txnId);
        }
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID, long lowWaterMark) {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();

        try {
            TxnBuffer txnBuffer = getTxnBufferOrThrowNotFoundException(txnID);
            txnBuffer.abort();
            buffers.remove(txnID, txnBuffer);
            abortFuture.complete(null);
        } catch (TransactionBufferException.TransactionNotFoundException
                | TransactionBufferException.TransactionStatusException e) {
            abortFuture.completeExceptionally(e);
        }

        return abortFuture;
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        List<TxnBuffer> buffersToPurge = new ArrayList<>();
        synchronized (txnIndex) {
            dataLedgers.forEach(ledger -> {
                Set<TxnID> txns = txnIndex.remove(ledger);
                txns.forEach(txnId -> {
                    TxnBuffer tb = buffers.remove(txnId);
                    if (null != tb) {
                        buffersToPurge.add(tb);
                    }
                });
            });
        }

        buffersToPurge.forEach(TxnBuffer::close);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> clearSnapshot() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        buffers.values().forEach(TxnBuffer::close);
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

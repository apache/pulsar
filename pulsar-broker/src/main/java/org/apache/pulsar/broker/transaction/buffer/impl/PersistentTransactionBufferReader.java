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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionStatusException;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.broker.transaction.buffer.TransactionEntry;
import org.apache.pulsar.broker.transaction.buffer.TransactionMeta;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

/**
 * A persistent transaction buffer reader implementation.
 */
@Slf4j
public class PersistentTransactionBufferReader implements TransactionBufferReader {

    static final long DEFAULT_START_SEQUENCE_ID = -1L;

    private final ManagedLedger ledger;
    private final TransactionMeta meta;
    private volatile long currentSequenceId = DEFAULT_START_SEQUENCE_ID;


    PersistentTransactionBufferReader(TransactionMeta meta, ManagedLedger ledger)
        throws TransactionNotSealedException {
        if (TxnStatus.OPEN == meta.status()) {
            throw new TransactionNotSealedException("Transaction `" + meta.id() + "` is not sealed yet");
        }
        this.meta = meta;
        this.ledger = ledger;
    }

    @Override
    public CompletableFuture<List<TransactionEntry>> readNext(int numEntries) {
        return meta.readEntries(numEntries, currentSequenceId)
                   .thenCompose(this::readEntry)
                   .thenApply(entries -> entries.stream()
                                                .sorted(Comparator.comparingLong(entry -> entry.sequenceId()))
                                                .collect(Collectors.toList()));
    }

    private CompletableFuture<List<TransactionEntry>> readEntry(SortedMap<Long, Position> entries) {
        CompletableFuture<List<TransactionEntry>> readFuture = new CompletableFuture<>();
        List<TransactionEntry> txnEntries = new ArrayList<>(entries.size());
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        int numMessageInTxn = -1;
        try {
            numMessageInTxn = meta.numMessageInTxn();
        } catch (TransactionStatusException e) {
            log.error("Get transaction totalBatchSize failed.", e);
            readFuture.completeExceptionally(e);
        }

        final int finalNumMessageInTxn = numMessageInTxn;
        for (Map.Entry<Long, Position> longPositionEntry : entries.entrySet()) {
            CompletableFuture<Void> tmpFuture = new CompletableFuture<>();
            readEntry(longPositionEntry.getValue()).whenComplete((entry, throwable) -> {
                if (null != throwable) {
                    tmpFuture.completeExceptionally(throwable);
                } else {
                    TransactionEntry txnEntry = new TransactionEntryImpl(meta.id(), longPositionEntry.getKey(),
                            entry, meta.committedAtLedgerId(), meta.committedAtEntryId(), finalNumMessageInTxn);
                    synchronized (txnEntries) {
                        txnEntries.add(txnEntry);
                    }
                    tmpFuture.complete(null);
                }
            });
            futures.add(tmpFuture);
        }

        FutureUtil.waitForAll(futures).whenComplete((ignore, error) -> {
            if (error != null) {
                readFuture.completeExceptionally(error);
            } else {
                currentSequenceId = entries.lastKey();
                readFuture.complete(txnEntries);
            }
        });

        return readFuture;
    }

    private CompletableFuture<Entry> readEntry(Position position) {
        CompletableFuture<Entry> readFuture = new CompletableFuture<>();

        ManagedLedgerImpl readLedger = (ManagedLedgerImpl) ledger;

        readLedger.asyncReadEntry((PositionImpl) position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                readFuture.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                readFuture.completeExceptionally(exception);
            }
        }, null);

        return readFuture;
    }

    @Override
    public void close() {
        log.info("Txn {} reader closed.", meta.id());
    }
}

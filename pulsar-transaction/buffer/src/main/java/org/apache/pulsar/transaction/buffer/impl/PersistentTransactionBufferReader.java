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
package org.apache.pulsar.transaction.buffer.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.transaction.buffer.TransactionEntry;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.EndOfTransactionException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionSealedException;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

/**
 * A persistent transaction buffer reader implementation.
 */
@Slf4j
public class PersistentTransactionBufferReader implements TransactionBufferReader {

    private final ManagedCursor readCursor;
    private final TransactionMeta meta;
    private long currentSeuquenceId = -1L;


    PersistentTransactionBufferReader(TransactionMeta meta, ManagedLedger ledger)
        throws ManagedLedgerException, TransactionNotSealedException {
        if (TxnStatus.OPEN == meta.status()) {
            throw new TransactionNotSealedException("Transaction `" + meta.id() + "` is not sealed yet");
        }
        this.meta = meta;
        this.readCursor = ledger.newNonDurableCursor(PositionImpl.earliest);
    }

    @Override
    public CompletableFuture<List<TransactionEntry>> readNext(int numEntries) {
        CompletableFuture<List<TransactionEntry>> readFuture = new CompletableFuture<>();

        meta.readEntries(numEntries, currentSeuquenceId).thenCompose(entries -> {
            readEntry(entries).thenCompose(txnEntries -> {
                readFuture.complete(txnEntries);
                return null;
            }).exceptionally(readError -> {
                readFuture.completeExceptionally(readError);
                return null;
            });
            return null;
        }).exceptionally(e -> {
            readFuture.completeExceptionally(e);
            return null;
        });

        return readFuture;
    }

    private CompletableFuture<List<TransactionEntry>> readEntry(SortedMap<Long, Position> entries) {
        CompletableFuture<List<TransactionEntry>> readFuture = new CompletableFuture<>();

        List<TransactionEntry> txnEntries = new ArrayList<>(entries.size());

        for (Map.Entry<Long, Position> longPositionEntry : entries.entrySet()) {
            readEntry(longPositionEntry.getValue()).thenApply(entry -> {
                TransactionEntry txnEntry = new TransactionEntryImpl(meta.id(), longPositionEntry.getKey(),
                                                                     entry.getDataBuffer(), meta.committedAtLedgerId(),
                                                                     meta.committedAtEntryId());
                txnEntries.add(txnEntry);
                return null;
            }).exceptionally(e -> {
                readFuture.completeExceptionally(e);
                return null;
            });

            if (readFuture.isCompletedExceptionally()) {
                return readFuture;
            }
        }

        readFuture.complete(txnEntries);

        return readFuture;
    }

    private CompletableFuture<Entry> readEntry(Position position) {
        CompletableFuture<Entry> readFuture = new CompletableFuture<>();

        readCursor.seek(position);
        if (readCursor.hasMoreEntries()) {
            readCursor.asyncReadEntries(1, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    readFuture.complete(entries.get(0));
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    readFuture.completeExceptionally(exception);
                }
            }, null);
        } else {
            readFuture.completeExceptionally(
                new EndOfTransactionException("No more entries found in transaction `" + meta.id() + "` log"));
            return readFuture;
        }

        return readFuture;
    }

    @Override
    public void close() {
        if (readCursor == null) {
            return;
        }
        readCursor.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                log.info("Transaction `{}` closed successfully.", meta.id());
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Transaction `{}` closed failed.", meta.id(), exception);
            }
        }, null);
    }
}

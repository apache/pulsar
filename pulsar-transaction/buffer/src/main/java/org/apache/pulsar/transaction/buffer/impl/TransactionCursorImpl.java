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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.transaction.buffer.TransactionCursor;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats;

// Transaction cursor maintains transaction indexes.
@Slf4j
public class TransactionCursorImpl implements TransactionCursor {

    private final ManagedLedger transactionLog;
    private ManagedCursor indexLogCursor;

    private ConcurrentMap<TxnID, TransactionMetaImpl> txnIndex;
    private Map<Long, Set<TxnID>> committedTxnIndex;

    private PulsarService pulsar;

    private volatile boolean reocvering;

    static class Result {
        Position position;
        ManagedLedgerException exception;
    }

    public TransactionCursorImpl(ManagedLedger managedLedger, PulsarService pulsar) {
        this.pulsar = pulsar;
        this.transactionLog = managedLedger;
        this.txnIndex = new ConcurrentHashMap<>();
        this.committedTxnIndex = new TreeMap<>();

        initializeTransactionCursor();
    }

    private void initializeTransactionCursor() {
        transactionLog.asyncOpenCursor(PersistentTransactionTopic.TRANSACTION_CURSOR_NAME,
           new AsyncCallbacks.OpenCursorCallback() {
               @Override
               public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                   cursor.setAlwaysInactive();
                   indexLogCursor = cursor;
                   recover();
               }

               @Override
               public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                   log.warn("Failed to open transaction index cursor: {}",
                            exception.getMessage());
               }
           }, null);
    }

    // Take snapshot will push all index messages into the index cursor
    //
    //           ----------------------------------------
    //                 ||       |        |     ||
    //             S1  || START | MIDDLE | END ||  S3
    //                 ||       |        |     ||
    //           ----------------------------------------
    //
    // This operation will triggered after new transaction committed.
    public void takeSnapshot(Position txnBufferPosition) {
        List<TransactionMeta> metas = txnIndex.values().stream().collect(Collectors.toList());
        try {
            Position start = beginTakeSnapshot(txnBufferPosition);
            indexSnapshot(start, metas);
            endTakeSnapshot();
        } catch (ManagedLedgerException e) {
            e.printStackTrace();
        }
    }

    private Position beginTakeSnapshot(Position position) throws ManagedLedgerException {
        return record(DataFormat.startStore(position));
    }

    private void indexSnapshot(Position position, List<TransactionMeta> snapshot) throws ManagedLedgerException {
        for (TransactionMeta meta : snapshot) {
            TransactionBufferDataFormats.StoredTxn storedTxn = DataFormat
                                                                   .middleStore(position, (TransactionMetaImpl) meta);
            record(storedTxn);
        }
    }

    private void endTakeSnapshot() throws ManagedLedgerException {
        record(DataFormat.endStore());
    }

    private Position record(TransactionBufferDataFormats.StoredTxn meta) throws ManagedLedgerException {
        Result result = new Result();

        indexLogCursor.asyncAddEntries(meta.toByteArray(), new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                result.position = position;
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
            } }, null);
        if (null != result.exception) {
            throw result.exception;
        }
        return result.position;
    }

    public void recover() {
        indexLogCursor.getLastEntryId().thenApply(entryId -> {
            recoverFromLedger(entryId);
            return null;
        }).exceptionally(e -> {
            log.warn("Failed to read last entry id from ledger. {}", e.getMessage());
            return null;
        });

    }

    private void recoverFromLedger(long entryId) {
        synchronized (this) {
            indexLogCursor.asyncReadEntry(entryId, new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(Entry entry, Object ctx) {
                    TransactionBufferDataFormats.StoredTxn storedTxn = DataFormat.parseStoredTxn(entry.getData());
                    switch (storedTxn.getStoredStatus()) {
                        case START:
                            if (reocvering) {
                                recoverFromTxnLog(storedTxn.getBufferPosition().getLedgerId(),
                                                  storedTxn.getBufferPosition().getEntryId());
                            } else {
                                pulsar.getExecutor().execute(() -> recoverFromLedger(entry.getEntryId() - 1));
                            }
                            break;
                        case MIDDLE:
                            if (reocvering) {
                                recoverIndex(entry);
                            }
                            pulsar.getExecutor().execute(() -> recoverFromLedger(entry.getEntryId() - 1));
                            break;
                        case END:
                            reocvering = true;
                            pulsar.getExecutor().execute(() -> recoverFromLedger(entry.getEntryId() - 1));
                    }
                }

                @Override
                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                    log.warn("");
                }
            }, null);
        }
    }

    private void recoverFromTxnLog(long ledgerId, long entryId) {
        PositionImpl snapshot = new PositionImpl(ledgerId, entryId);
        try {
            ManagedCursor cursor = transactionLog.newNonDurableCursor(snapshot);
            replayTransactionLog(cursor);
        } catch (ManagedLedgerException e) {
            e.printStackTrace();
        }

    }

    private void replayTransactionLog(ManagedCursor cursor) {
        cursor.asyncReadEntries(100, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                // read transaction log and replay all messages to recover index
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("");
            }
        }, null);
    }

    private synchronized void recoverIndex(Entry entry) {
        TransactionMetaImpl meta = (TransactionMetaImpl) DataFormat.parseToTransactionMeta(entry.getData());
        txnIndex.put(meta.getTxnID(), meta);
        committedTxnIndex.computeIfAbsent(meta.getCommittedAtLedgerId(), txnId -> new HashSet<>()).add(meta.getTxnID());
    }



    @Override
    public TransactionMeta getTxnMeta(TxnID txnID) throws TransactionNotFoundException {
        TransactionMetaImpl transactionMeta = txnIndex.get(txnID);
        if (null == transactionMeta) {
            throw new TransactionNotFoundException("Transaction `" + txnID + "` doesn't exist");
        }
        return transactionMeta;
    }

    @Override
    public TransactionMeta getOrCreateTxnMeta(TxnID txnID) {
        TransactionMetaImpl meta = txnIndex.get(txnID);
        if (null == meta) {
            TransactionMetaImpl newMeta = new TransactionMetaImpl(txnID);
            txnIndex.put(txnID, newMeta);
            return newMeta;
        }
        return meta;
    }

    public void addTxnToCommittedIndex(long committedAtLedgerId, TxnID txnID, Position currentTxnLogPosition) {
        committedTxnIndex.computeIfAbsent(committedAtLedgerId, txn -> new HashSet<>()).add(txnID);
        pulsar.getExecutor().execute(() -> takeSnapshot(currentTxnLogPosition));
    }

}

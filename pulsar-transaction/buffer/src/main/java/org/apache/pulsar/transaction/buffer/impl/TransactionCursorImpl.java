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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarMarkers;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.buffer.TransactionCursor;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.NoTxnsCommittedAtLedgerException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionIndexRecoveringError;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.proto.TransactionBufferDataFormats.StoredTxn;

@Slf4j
public class TransactionCursorImpl implements TransactionCursor {

    private final ManagedLedger txnLog;
    private volatile AtomicReference<ManagedCursor> indexCursor = new AtomicReference<>();

    private final ConcurrentMap<TxnID, TransactionMetaImpl> txnIndex;
    private final Map<Long, Set<TxnID>> committedLedgerTxnIndex;

    TransactionCursorImpl(ManagedLedger ledger) throws InterruptedException {
        this.txnIndex = new ConcurrentHashMap<>();
        this.committedLedgerTxnIndex = new TreeMap<>();
        this.txnLog = ledger;
        initializeTransactionCursor();
    }

    @VisibleForTesting
    void addToTxnIndex(TransactionMetaImpl meta) {
        txnIndex.putIfAbsent(meta.id(), meta);
    }

    @VisibleForTesting
    void addToCommittedLedgerTxnIndex(long ledgerId, TxnID txnID) {
        committedLedgerTxnIndex.computeIfAbsent(ledgerId, ledger -> new HashSet<>()).add(txnID);
    }

    @VisibleForTesting
    TransactionMetaImpl findInIndex(TxnID txnID) {
        return txnIndex.get(txnID);
    }

    @VisibleForTesting
    LedgerHandle getCursorLedger() {
        return indexCursor.get().getCurrentCursorLedger();
    }

    private void initializeTransactionCursor() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        txnLog.asyncOpenCursor(PersistentTransactionBuffer.TXN_CURSOR_NAME, new AsyncCallbacks.OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                cursor.setAlwaysInactive();
                indexCursor.compareAndSet(null, cursor);
                recover().whenComplete((ignore, error) -> {
                    if (error != null) {
                        log.error("Failed to recover the transaction index");
                    } else {
                        log.info("Succeed to recover the transaction index.");
                    }
                    latch.countDown();
                });
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Failed to open the transaction index cursor to recover transaction index", exception);
                latch.countDown();
            }
        }, null);

        latch.await();
    }

    @Override
    public CompletableFuture<TransactionMeta> getTxnMeta(TxnID txnID, boolean createIfNotExist) {
        CompletableFuture<TransactionMeta> getFuture = new CompletableFuture<>();
        TransactionMeta meta = txnIndex.get(txnID);
        if (null == meta) {
            if (!createIfNotExist) {
                getFuture.completeExceptionally(
                    new TransactionNotFoundException("Transaction `" + txnID + "` doesn't" + " exist"));
                return getFuture;
            }

            TransactionMetaImpl newMeta = new TransactionMetaImpl(txnID);
            TransactionMeta oldMeta = txnIndex.putIfAbsent(txnID, newMeta);
            if (null != oldMeta) {
                meta = oldMeta;
            } else {
                meta = newMeta;
            }
        }
        getFuture.complete(meta);

        return getFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(long committedLedgerId, long committedEntryId, TxnID txnID,
                                             Position position) {
        return getTxnMeta(txnID, false)
            .thenCompose(meta -> meta.commitTxn(committedLedgerId, committedEntryId))
            .thenAccept(meta -> addTxnToCommittedIndex(txnID, committedLedgerId));
    }

    private void addTxnToCommittedIndex(TxnID txnID, long committedAtLedgerId) {
        synchronized (committedLedgerTxnIndex) {
            committedLedgerTxnIndex.computeIfAbsent(committedAtLedgerId, ledgerId -> new HashSet<>()).add(txnID);
        }
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID) {
        return getTxnMeta(txnID, false)
            .thenCompose(meta -> meta.abortTxn())
            .thenApply(meta -> null);
    }

    public CompletableFuture<Set<TxnID>> getAllTxnsCommittedAtLedger(long ledgerId) {
        CompletableFuture<Set<TxnID>> removeFuture = new CompletableFuture<>();

        Set<TxnID> txnIDS = committedLedgerTxnIndex.get(ledgerId);

        if (null == txnIDS) {
            removeFuture.completeExceptionally(new NoTxnsCommittedAtLedgerException(
                "Transaction committed ledger id `" + ledgerId + "` doesn't exist") {
            });
            return removeFuture;
        }

        removeFuture.complete(txnIDS);
        return removeFuture;
    }

    @Override
    public CompletableFuture<Void> removeTxnsCommittedAtLedger(long ledgerId) {
        CompletableFuture<Void> removeFuture = new CompletableFuture<>();

        synchronized (committedLedgerTxnIndex) {
            Set<TxnID> txnIDS = committedLedgerTxnIndex.remove(ledgerId);
            if (null == txnIDS) {
                removeFuture.completeExceptionally(new NoTxnsCommittedAtLedgerException(
                    "Transaction committed ledger id `" + ledgerId + "` doesn't exist"));
            } else {
                txnIDS.forEach(txnID -> {
                    txnIndex.remove(txnID);
                });
                removeFuture.complete(null);
            }
        }

        return removeFuture;
    }

    // Take a snapshot for all indexes. We can persist the transaction meta because the indexes can be rebuilt by it.
    // a. Create a begin block and put the current transaction log position into it.
    // b. Create the middle  block to store the transaction meta  and the snapshot start position.
    // c. Create the end block to say the snapshot is ending and put the sanpshot start position to get the number of
    //    snapshot blocks  when recovering.
    public CompletableFuture<Void> takeSnapshot(Position txnBufferPosition) {
        return startSnapshot(txnBufferPosition)
            .thenCompose(position -> indexSnapshot(position, txnIndex.values()))
            .thenCompose(position -> endSnapshot(position));
    }

    private CompletableFuture<Position> startSnapshot(Position position) {
        return record(DataFormat.startStore(position));
    }

    private CompletableFuture<Position> indexSnapshot(Position startSnapshotPos,
                                               Collection<TransactionMetaImpl> snapshotsMeta) {
        List<CompletableFuture<Position>> snapshot =
            snapshotsMeta.stream()
                         .map(meta -> record(DataFormat.middleStore(startSnapshotPos, (TransactionMetaImpl) meta)))
                         .collect(Collectors.toList());

        return FutureUtil.waitForAll(snapshot).thenApply(ignore -> startSnapshotPos);
    }

    private CompletableFuture<Void> endSnapshot(Position startPos) {
        return record(DataFormat.endStore(startPos)).thenApply(position -> null);
    }

    private CompletableFuture<Position> record(StoredTxn storedTxn) {
        CompletableFuture<Position> recordFuture = new CompletableFuture<>();

        indexCursor.get().asyncAddEntry(storedTxn.toByteArray(), new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.info("Success to record the txn [{} - {}:{}] at [{}]", storedTxn.getStoredStatus(),
                             storedTxn.getTxnMeta().getTxnId().getMostSigBits(),
                             storedTxn.getTxnMeta().getTxnId().getLeastSigBits(), position);
                }
                recordFuture.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                if (log.isDebugEnabled()) {
                    log.info("Failed to record the txn [{} : {}:{}]", storedTxn.getStoredStatus(),
                             storedTxn.getTxnMeta().getTxnId().getMostSigBits(),
                             storedTxn.getTxnMeta().getTxnId().getLeastSigBits());
                }
                recordFuture.completeExceptionally(exception);
            }
        }, null);

        return recordFuture;
    }

    // Recover the index.
    // i. Read the last entry of the transaction cursor ledger.
    //      a. If the end entry is the beginning of the snapshot, move backward and recover the index by c.
    //      b. If the end entry in the middle of the snapshot, get teh snapshot beginning position, recover the index
    //         by a.
    //      c. If the end entry is the ending of the snapshot, get the snapshot beginning position, recover it by the
    //         middle  entries.
    public CompletableFuture<Void> recover() {
        CompletableFuture<Void> recoverFuture = new CompletableFuture<>();

        LedgerHandle lh = indexCursor.get().getCurrentCursorLedger();
        long ledgerId = lh.getId();
        long entryId = lh.getLastAddConfirmed();
        PositionImpl currentPosition = PositionImpl.get(ledgerId, entryId);

        readSpecifiedPosEntry(currentPosition)
            .thenApply(entry -> new PersistentTxnIndexSnapshot(entry.getData()))
            .whenComplete((snapshot, throwable) -> {
                if (throwable != null) {
                    recoverFuture.completeExceptionally(throwable);
                } else {
                    if (snapshot.status == null) {
                        recoverFuture.complete(null);
                    } else {
                        switch (snapshot.status) {
                            case START:
                                recoverFromStart(currentPosition).whenComplete((ignore, error) -> {
                                    checkComplete(error, recoverFuture);
                                });
                                break;
                            case MIDDLE:
                                recoverFromMiddle(snapshot).whenComplete((ignore, error) -> {
                                    checkComplete(error, recoverFuture);
                                });
                                break;
                            case END:
                                recoverFromEnd(snapshot).whenComplete((ignore, error) -> {
                                    checkComplete(error, recoverFuture);
                                });
                        }
                    }
                }
            });

        return recoverFuture;
    }

    private void checkComplete(Throwable error, CompletableFuture<Void> future) {
        if (error != null) {
            future.completeExceptionally(error);
        } else {
            future.complete(null);
        }
    }

    @Getter
    final static class PersistentTxnIndexSnapshot {
        enum SnapshotStatus  {
            START,
            MIDDLE,
            END,
        }

        SnapshotStatus status;
        // If the status is START, the position is the position which is the transaction log doing snapshot.
        //  If the status is others, the position is the snapshot beginning position on the cursor ledger.
        Position position;
        TransactionMetaImpl meta;

        PersistentTxnIndexSnapshot(byte[] entry) {
            StoredTxn txn = DataFormat.parseStoredTxn(entry);
            switch (txn.getStoredStatus()) {
                case START:
                    this.status = SnapshotStatus.START;
                    break;
                case MIDDLE:
                    this.status = SnapshotStatus.MIDDLE;
                    break;
                case END:
                    this.status = SnapshotStatus.END;
                    break;
            }
            this.meta = DataFormat.recoverMeta(txn.getTxnMeta());
            this.position = DataFormat.recoverPosition(txn.getPosition());
        }

    }

    private CompletableFuture<Void> recoverFromStart(Position currentPosition) {
        return readPrevEntry(currentPosition)
                   .thenApply(entry -> new PersistentTxnIndexSnapshot(entry.getData()))
                   .thenCompose(this::recoverFromEnd);

    }

    private CompletableFuture<Entry> readPrevEntry(Position position) {
        PositionImpl currentPos = (PositionImpl) position;
        if (currentPos.getEntryId() == 0) {
            return FutureUtil.failedFuture(
                new TransactionIndexRecoveringError("Not found the prev position of the current position " + position));
        }

        PositionImpl prevPosition = PositionImpl.get(currentPos.getLedgerId(), currentPos.getEntryId() - 1);
        return readSpecifiedPosEntry(prevPosition);
    }

    private CompletableFuture<Void> recoverFromMiddle(PersistentTxnIndexSnapshot snapshot) {
        return readSpecifiedPosEntry(snapshot.position)
                   .thenApply(entry -> new PersistentTxnIndexSnapshot(entry.getData()))
                   .thenCompose(startBlock -> recoverFromStart(snapshot.position));

    }

    private CompletableFuture<Entry> readSpecifiedPosEntry(Position position) {
        CompletableFuture<Entry> readFuture = new CompletableFuture<>();

        PositionImpl readPos = (PositionImpl) position;
        LedgerHandle ledger = indexCursor.get().getCurrentCursorLedger();

        ledger.asyncReadEntries(readPos.getEntryId(), readPos.getEntryId(), (rc, handle, entries, ctx) -> {
            if (rc != BKException.Code.OK) {
                readFuture.completeExceptionally(BKException.create(rc));
            } else {
                if (entries.hasMoreElements()) {
                    LedgerEntry ledgerEntry = entries.nextElement();
                    EntryImpl entry = EntryImpl.create(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId(),
                                                       ledgerEntry.getEntry());

                    readFuture.complete(entry);
                } else {
                    readFuture.completeExceptionally(new NoSuchElementException(
                        "No such entry " + readPos.getEntryId() + " in ledger " + handle.getId()));
                }
            }
        }, null);

        return readFuture;
    }

    private CompletableFuture<Void> recoverFromEnd(PersistentTxnIndexSnapshot snapshot) {
        return recoverFromLedger(snapshot);
    }

    private CompletableFuture<Void> recoverFromLedger(PersistentTxnIndexSnapshot snapshot) {
        return readEntryFromCursorLedger(snapshot.position, indexCursor.get().getCurrentCursorLedger())
            .thenApply(entries ->
                           entries.stream()
                                  .map(entry -> new PersistentTxnIndexSnapshot(entry.getData()))
                                  .filter(tmpSnapshot ->
                                              !tmpSnapshot.getStatus()
                                                         .equals(PersistentTxnIndexSnapshot.SnapshotStatus.END))
                                  .collect(Collectors.toList()))
            .thenCompose(snapshots -> rebuildIndex(snapshots))
            .thenCompose(beginning -> replayTxnLogEntries(beginning.position));
    }

    private CompletableFuture<PersistentTxnIndexSnapshot> rebuildIndex(List<PersistentTxnIndexSnapshot> snapshots) {
        List<CompletableFuture<Void>> rebuildFutre = new ArrayList<>();

        snapshots.stream()
                 .filter(snapshot -> snapshot.getStatus().equals(PersistentTxnIndexSnapshot.SnapshotStatus.MIDDLE))
                 .map(snapshot -> snapshot.getMeta())
                 .forEach(transactionMeta -> rebuildFutre.add(rebuildIndexByEntry(transactionMeta)));

        return FutureUtil.waitForAll(rebuildFutre).thenCompose(ignore -> findStart(snapshots));
    }

    private CompletableFuture<PersistentTxnIndexSnapshot> findStart(List<PersistentTxnIndexSnapshot> snapshots) {
        List<PersistentTxnIndexSnapshot> beginning = snapshots.stream()
                 .filter(snapshot -> snapshot.getStatus().equals(PersistentTxnIndexSnapshot.SnapshotStatus.START))
                 .collect(Collectors.toList());

        if (beginning.size() != 1 || beginning.get(0) == null) {
            return FutureUtil.failedFuture(new TransactionIndexRecoveringError(
                "Found more than one START when recovering transaction index on cursor ledger: "
                + indexCursor.get().getCurrentCursorLedger().getId()));
        }

        return CompletableFuture.completedFuture(beginning.get(0));
    }

    private CompletableFuture<Void> rebuildIndexByEntry(TransactionMetaImpl meta) {
        // add to transaction index
        txnIndex.putIfAbsent(meta.id(), meta);

        // add to committed ledger transaction index
        synchronized (committedLedgerTxnIndex) {
            if (meta.isCommitted()) {
                addTxnToCommittedIndex(meta.id(), meta.committedAtLedgerId());
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<List<Entry>> readEntryFromCursorLedger(Position startSnapshotPos,
                                                                     LedgerHandle cursorLedger) {
        CompletableFuture<List<Entry>> readFuture = new CompletableFuture<>();
        PositionImpl startPos = (PositionImpl) startSnapshotPos;
        long startEntryId = startPos.getEntryId();
        long endEntryId = cursorLedger.getLastAddConfirmed();

        cursorLedger.asyncReadEntries(startEntryId, endEntryId, (rc, handle, entries, ctx) -> {
            if (rc != BKException.Code.OK) {
                readFuture.completeExceptionally(BKException.create(rc));
            } else {
                if (entries.hasMoreElements()) {
                    List<Entry> entryList = Collections.list(entries)
                                                       .stream()
                                                       .map(ledgerEntry -> EntryImpl.create(ledgerEntry.getLedgerId()
                                                           , ledgerEntry.getEntryId(), ledgerEntry.getEntry()))
                                                       .collect(Collectors.toList());
                    readFuture.complete(entryList);
                } else {
                    readFuture.completeExceptionally(new NoSuchElementException(
                        "No more entry can read from ledger: " + handle.getId() + ", entry: " + startEntryId));
                }
            }
        }, null);

        return readFuture;
    }

    private CompletableFuture<List<Entry>> readEntryFromLedger(Position startSnapshotPos, ManagedLedger managedLedger) {
        CompletableFuture<List<Entry>> readFuture = new CompletableFuture<>();

        List<CompletableFuture<Void>> readAllEntryFuture = new ArrayList<>();
        List<Entry> entryList = new ArrayList<>();
        ManagedLedger cursorLedger = managedLedger;
        ManagedCursor readCursor = null;
        try {
            readCursor = cursorLedger.newNonDurableCursor(startSnapshotPos);

            while (readCursor.hasMoreEntries()) {
                CompletableFuture<Void> readEntries = new CompletableFuture<>();
                readCursor.asyncReadEntries(100, new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        synchronized (entryList) {
                            entryList.addAll(entries);
                        }
                        readEntries.complete(null);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        readEntries.completeExceptionally(exception);
                    }
                }, null);
                readAllEntryFuture.add(readEntries);
            }

            FutureUtil.waitForAll(readAllEntryFuture).whenComplete((ignore, error) -> {
                if (error != null) {
                    readFuture.completeExceptionally(error);
                } else {
                    readFuture.complete(entryList);
                }
            });

        } catch (ManagedLedgerException e) {
            readFuture.completeExceptionally(e);
        } finally {
            if (readCursor != null) {
                readCursor.asyncClose(new AsyncCallbacks.CloseCallback() {
                    @Override
                    public void closeComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("Cursor for recovering the transaction index is closed");
                        }
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed close the cursor for recovering the transaction index.", exception);
                    }
                }, null);
            }
        }
        return readFuture;
    }

    // Replay all messages from the previous snapshot position on the transaction log.
    private CompletableFuture<Void> replayTxnLogEntries(Position position) {
        return readEntryFromLedger(position, txnLog).thenAccept(entries -> entries.forEach(this::replayEntry));
    }

    private CompletableFuture<Void> replayEntry(Entry entry) {
        PulsarApi.MessageMetadata messageMetadata = Commands.parseMessageMetadata(entry.getDataBuffer());

        TxnID txnID = new TxnID(messageMetadata.getTxnidMostBits(), messageMetadata.getTxnidLeastBits());
        long sequenceId = messageMetadata.getSequenceId();

        switch (messageMetadata.getMarkerType()) {
            case PulsarMarkers.MarkerType.TXN_COMMIT_VALUE:
                return replayCommitMarker(txnID, entry);
            case PulsarMarkers.MarkerType.TXN_ABORT_VALUE:
                return abortTxn(txnID);
            default:
                return getTxnMeta(txnID, true)
                           .thenCompose(meta -> meta.appendEntry(sequenceId, entry.getPosition()));
        }
    }

    private CompletableFuture<Void> replayCommitMarker(TxnID txnID, Entry entry) {
        try {
            PulsarMarkers.TxnCommitMarker marker = Markers.parseCommitMarker(entry.getDataBuffer());
            long committedLedger = marker.getMessageId().getLedgerId();
            long committedEntry = marker.getMessageId().getEntryId();

            return commitTxn(committedLedger, committedEntry, txnID, entry.getPosition());
        } catch (IOException e) {
            return FutureUtil.failedFuture(e);
        }
    }
}

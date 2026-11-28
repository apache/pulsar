/*
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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ResetCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorResetInProgressException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedCursorResetAcknowledgmentTest extends MockedBookKeeperTestCase {

    @DataProvider
    public Object[][] pendingRead() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "pendingRead", timeOut = 30000)
    public void individualAckDuringDurableReset(boolean pendingRead) throws Exception {
        ManagedLedger ledger = factory.open("reset-ack-race",
                defaultConfig().setPersistIndividualAckAsLongArray(false));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        cursor.setInactive(); // Send the read to BookKeeper instead of serving it from cache.
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        cursor.markDelete(positions.get(4));
        Position resetMarkDelete = positions.get(1);
        Position ackPosition = positions.get(6);
        long lastEntryBeforeReset = cursor.getCursorLedgerLastEntry();

        CompletableFuture<Void> releaseReset = bkc.addEntryResponseGate();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        AtomicInteger ackCallbacks = new AtomicInteger();
        try {
            CompletableFuture<Void> reset = resetCursor(cursor, positions.get(2));
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(cursor.getCursorLedgerLastEntry()).isEqualTo(lastEntryBeforeReset + 1));
            assertThat(reset).isNotDone();

            CompletableFuture<Void> read = CompletableFuture.completedFuture(null);
            if (pendingRead) {
                CompletableFuture<Void> readStarted = new CompletableFuture<>();
                bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
                    readStarted.complete(null);
                    return releaseRead.thenApply(__ -> entries);
                });
                read = ManagedLedgerUtils.readEntries(cursor, 1, PositionFactory.LATEST)
                        .thenAccept(entries -> entries.forEach(Entry::release));
                readStarted.get(5, TimeUnit.SECONDS);
                assertThat(cursor.getPendingReadOpsCount()).isEqualTo(1);
            }

            CompletableFuture<Void> resetCompletion = rejectDeleteDuringReset(cursor, ackPosition, ackCallbacks)
                    .getResetCompletion().toCompletableFuture();
            assertThat(resetCompletion).isNotDone();
            assertThat(cursor.isMessageDeleted(ackPosition)).isFalse();

            releaseReset.complete(null);
            reset.get(5, TimeUnit.SECONDS);
            resetCompletion.get(5, TimeUnit.SECONDS);
            releaseRead.complete(null);
            read.get(5, TimeUnit.SECONDS);

            // Check the record recovery would read before another write (including close) can replace it.
            PositionInfo checkpoint = readCursorCheckpoint(cursor);
            assertThat(PositionFactory.create(checkpoint.getLedgerId(), checkpoint.getEntryId()))
                    .isEqualTo(resetMarkDelete);
            assertThat(checkpoint.getIndividualDeletedMessagesCount()).isZero();
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(resetMarkDelete);
            assertThat(cursor.isMessageDeleted(ackPosition)).isFalse();

            cursor.delete(ackPosition);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(resetMarkDelete);
            assertThat(cursor.isMessageDeleted(ackPosition)).isTrue();
            assertThat(ackCallbacks).hasValue(1);
            PositionInfo checkpointAfterAck = readCursorCheckpoint(cursor);
            assertThat(PositionFactory.create(checkpointAfterAck.getLedgerId(), checkpointAfterAck.getEntryId()))
                    .isEqualTo(resetMarkDelete);
            assertThat(checkpointAfterAck.getIndividualDeletedMessagesCount()).isEqualTo(1);
            MessageRange ackRange = checkpointAfterAck.getIndividualDeletedMessageAt(0);
            assertThat(PositionFactory.create(ackRange.getUpperEndpoint().getLedgerId(),
                    ackRange.getUpperEndpoint().getEntryId())).isEqualTo(ackPosition);
        } finally {
            releaseReset.complete(null);
            releaseRead.complete(null);
            bkc.setReadHandleInterceptor(null);
        }
    }

    @DataProvider
    public Object[][] resetCheckpointStorage() {
        return new Object[][] {{false, false, 0b10L}, {false, true, 0b10L}, {true, false, 0b10L}, {false, false, 0L}};
    }

    @Test(dataProvider = "resetCheckpointStorage", timeOut = 30000)
    public void resetCheckpointMatchesAcknowledgmentsAfterSeek(boolean metadataFallback, boolean persistLongRanges,
            long resetMaskWord) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setPersistIndividualAckAsLongArray(persistLongRanges);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-ack-checkpoint", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        Position resetMarkDelete = positions.getFirst();
        Position readPosition = positions.get(1);
        Position wholeEntry = positions.get(4);
        Position oldBatch = positions.get(5);
        long[] resetMask = resetMaskWord == 0 ? new long[0] : new long[] {resetMaskWord};

        cursor.markDelete(positions.get(2));
        cursor.delete(List.of(wholeEntry, AckSetStateUtil.createPositionWithAckSet(
                oldBatch.getLedgerId(), oldBatch.getEntryId(), new long[] {0b01L})));
        assertThat(cursor.isMessageDeleted(wholeEntry)).isTrue();
        assertThat(cursor.getBatchPositionAckSet(oldBatch)).containsExactly(0b01L);
        PositionInfo beforeReset = readCursorCheckpoint(cursor);
        assertThat(beforeReset.getIndividualDeletedMessagesCount()).isEqualTo(persistLongRanges ? 0 : 1);
        assertThat(beforeReset.getIndividualDeletedMessageRangesCount()).isEqualTo(persistLongRanges ? 1 : 0);
        assertThat(beforeReset.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(1);

        if (metadataFallback) {
            bkc.addEntryFailAfter(0, BKException.Code.WriteException);
        }
        Position seek = AckSetStateUtil.createPositionWithAckSet(
                readPosition.getLedgerId(), readPosition.getEntryId(), resetMask);
        resetCursor(cursor, seek).get(5, TimeUnit.SECONDS);
        assertResetAckState(cursor, resetMarkDelete, readPosition, wholeEntry, oldBatch, resetMask);

        // Inspect the reset's record before close can replace it with a fresh snapshot of the in-memory state.
        BatchedEntryDeletionIndexInfo batchCheckpoint;
        if (metadataFallback) {
            ManagedCursorInfo checkpoint = readCursorInfo(ledger, cursor.getName());
            assertThat(checkpoint.getCursorsLedgerId()).isEqualTo(-1);
            assertThat(PositionFactory.create(checkpoint.getMarkDeleteLedgerId(), checkpoint.getMarkDeleteEntryId()))
                    .isEqualTo(resetMarkDelete);
            assertThat(checkpoint.getIndividualDeletedMessagesCount()).isZero();
            assertThat(checkpoint.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(1);
            batchCheckpoint = checkpoint.getBatchedEntryDeletionIndexInfoAt(0);
        } else {
            PositionInfo checkpoint = readCursorCheckpoint(cursor);
            assertThat(PositionFactory.create(checkpoint.getLedgerId(), checkpoint.getEntryId()))
                    .isEqualTo(resetMarkDelete);
            assertThat(checkpoint.getIndividualDeletedMessagesCount()).isZero();
            assertThat(checkpoint.getIndividualDeletedMessageRangesCount()).isZero();
            assertThat(checkpoint.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(1);
            batchCheckpoint = checkpoint.getBatchedEntryDeletionIndexInfoAt(0);
        }
        assertThat(PositionFactory.create(batchCheckpoint.getPosition().getLedgerId(),
                batchCheckpoint.getPosition().getEntryId())).isEqualTo(readPosition);
        assertThat(batchCheckpoint.getDeleteSetsCount()).isEqualTo(1);
        assertThat(batchCheckpoint.getDeleteSetAt(0)).isEqualTo(resetMaskWord);

        // Recover from the reset checkpoint without closing the writer and rewriting the checkpoint first.
        ManagedCursorImpl recovered = new ManagedCursorImpl(bkc, ledger, cursor.getName());
        try {
            recoverCursor(recovered);
            assertResetAckState(recovered, resetMarkDelete, readPosition, wholeEntry, oldBatch, resetMask);
        } finally {
            recovered.closeReadOnlyCursorLedger();
        }
    }

    private static void assertResetAckState(ManagedCursorImpl cursor, Position markDelete, Position readPosition,
            Position wholeEntry, Position oldBatch, long[] resetMask) {
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(markDelete);
        assertThat(cursor.getReadPosition()).isEqualTo(readPosition);
        assertThat(cursor.isMessageDeleted(wholeEntry)).isFalse();
        assertThat(cursor.getBatchPositionAckSet(oldBatch)).isNull();
        assertThat(cursor.getBatchPositionAckSet(readPosition)).containsExactly(resetMask);
    }

    @Test(dataProvider = "pendingRead", timeOut = 30000)
    public void compactionResetKeepsPropertyChangesMadeWhileCheckpointIsInFlight(boolean pendingRead) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("compaction-reset-properties", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("__compaction");
        cursor.setInactive();
        Position readPosition = ledger.addEntry(new byte[] {0});
        Position previousMarkDelete = ledger.addEntry(new byte[] {1});
        Position markDelete = ledger.addEntry(new byte[] {2});

        Map<String, Long> properties = Map.of("CompactedTopicLedger", 123456L, "removed", 1L);
        if (pendingRead) {
            cursor.markDelete(previousMarkDelete, Map.of("CompactedTopicLedger", 123L));
        } else {
            cursor.markDelete(markDelete, properties);
        }
        long lastCheckpoint = cursor.getCursorLedgerLastEntry();
        Map<String, Long> updated = Map.of("CompactedTopicLedger", 123456L, "added", 2L);

        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        CompletableFuture<Void> releaseReset = bkc.addEntryResponseGate();
        try {
            CompletableFuture<Void> read = CompletableFuture.completedFuture(null);
            CompletableFuture<Void> ack = CompletableFuture.completedFuture(null);
            if (pendingRead) {
                CompletableFuture<Void> readStarted = new CompletableFuture<>();
                bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
                    readStarted.complete(null);
                    return releaseRead.thenApply(ignored -> entries);
                });
                read = ManagedLedgerUtils.readEntries(cursor, 1, PositionFactory.LATEST)
                        .thenAccept(entries -> entries.forEach(Entry::release));
                readStarted.get(5, TimeUnit.SECONDS);
                ack = ManagedLedgerUtils.markDelete(cursor, markDelete, properties);
                assertThat(ack).isNotDone();
            }
            CompletableFuture<Void> reset = resetCursor(cursor, readPosition);
            if (pendingRead) {
                ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
                releaseRead.complete(null);
                read.get(5, TimeUnit.SECONDS);
            }
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    cursor.getCursorLedgerLastEntry() == lastCheckpoint + 1
                            && releaseReset.getNumberOfDependents() > 0);
            assertThat(reset).isNotDone();
            assertThat(cursor.getProperties()).isEqualTo(properties);

            assertThat(cursor.putProperty("added", 2L)).isTrue();
            assertThat(cursor.removeProperty("removed")).isTrue();
            assertThat(cursor.getProperties()).isEqualTo(updated);

            releaseReset.complete(null);
            reset.get(5, TimeUnit.SECONDS);
            ack.get(5, TimeUnit.SECONDS);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(markDelete);
            assertThat(cursor.getPersistentMarkDeletedPosition()).isEqualTo(markDelete);
            assertThat(cursor.getReadPosition()).isEqualTo(readPosition);
            assertThat(cursor.getProperties()).isEqualTo(updated);
        } finally {
            releaseReset.complete(null);
            releaseRead.complete(null);
            bkc.setReadHandleInterceptor(null);
        }
    }

    @Test(timeOut = 30000)
    public void queuedCompactionResetPersistsPropertyUpdatesBeforeSubmission() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("queued-compaction-reset-properties",
                defaultConfig().setThrottleMarkDelete(0));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("__compaction");
        cursor.setInactive();
        Position committed = ledger.addEntry(new byte[] {0});
        ledger.addEntry(new byte[] {1});
        cursor.markDelete(committed, Map.of("CompactedTopicLedger", 11L, "removed", 1L));

        CompletableFuture<Void> readStarted = new CompletableFuture<>();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
            readStarted.complete(null);
            return releaseRead.thenApply(ignored -> entries);
        });
        try {
            CompletableFuture<Void> read = ManagedLedgerUtils.readEntries(cursor, 1, PositionFactory.LATEST)
                    .thenAccept(entries -> entries.forEach(Entry::release));
            readStarted.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> reset = resetCursor(cursor, committed);
            ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
            synchronized (cursor.pendingMarkDeleteOps) {
                assertThat(cursor.pendingMarkDeleteOps).hasSize(1);
            }
            assertThat(cursor.putProperty("added", 2L)).isTrue();
            assertThat(cursor.removeProperty("removed")).isTrue();
            releaseRead.complete(null);
            CompletableFuture.allOf(read, reset).get(5, TimeUnit.SECONDS);

            Map<String, Long> expected = Map.of("CompactedTopicLedger", 11L, "added", 2L);
            assertThat(cursor.getProperties()).isEqualTo(expected);
            ManagedCursorImpl recovered = new ManagedCursorImpl(bkc, ledger, cursor.getName());
            try {
                recoverCursor(recovered);
                assertThat(recovered.getMarkDeletedPosition()).isEqualTo(committed);
                assertThat(recovered.getProperties()).isEqualTo(expected);
            } finally {
                recovered.closeReadOnlyCursorLedger();
            }
        } finally {
            releaseRead.complete(null);
            bkc.setReadHandleInterceptor(null);
        }
    }

    @DataProvider
    public Object[][] retryCompactionReset() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "retryCompactionReset", timeOut = 30000)
    public void failedCompactionAckAndResetKeepPreviousCheckpointOnRecovery(boolean retryReset) throws Exception {
        String ledgerName = "failed-compaction-ack-and-reset";
        String cursorName = "__compaction";
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(cursorName);
        cursor.setInactive();
        Position committed = ledger.addEntry(new byte[] {0});
        ledger.addEntry(new byte[] {1});
        Position next = ledger.addEntry(new byte[] {2});
        cursor.markDelete(committed, Map.of("CompactedTopicLedger", 11L, "removed", 1L, "previous", 2L));
        Map<String, Long> replacement = Map.of("CompactedTopicLedger", 22L, "removed", 3L, "provisional", 4L);

        CompletableFuture<Void> readStarted = new CompletableFuture<>();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
            readStarted.complete(null);
            return releaseRead.thenApply(ignored -> entries);
        });
        CompletableFuture<Void> read = ManagedLedgerUtils.readEntries(cursor, 1, PositionFactory.LATEST)
                .thenAccept(entries -> entries.forEach(Entry::release));
        CompletableFuture<Void> failCheckpoint = bkc.addEntryWriteGate(cursor.getCursorLedger());
        BKException failure = BKException.create(BKException.Code.WriteException);
        try {
            readStarted.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> ack = ManagedLedgerUtils.markDelete(cursor, next, replacement);
            assertThat(ack).isNotDone();
            CompletableFuture<Void> reset = resetCursor(cursor, committed);
            ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(reset).isNotDone();
            releaseRead.complete(null);
            read.get(5, TimeUnit.SECONDS);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(cursor.getProperties()).isEqualTo(replacement));
            assertThat(cursor.putProperty("added", 5L)).isTrue();
            assertThat(cursor.removeProperty("removed")).isTrue();
            Position readPosition = cursor.getReadPosition();

            String cursorPath = "/managed-ledgers/" + ledgerName + "/" + cursorName;
            metadataStore.failConditional(new MetadataStoreException("reset checkpoint failed"),
                    (op, path) -> op == OperationType.PUT && path.equals(cursorPath));
            failCheckpoint.completeExceptionally(failure);
            assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(InvalidCursorPositionException.class);
            assertThatThrownBy(() -> ack.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.class);

            Map<String, Long> expected = Map.of("CompactedTopicLedger", 11L, "previous", 2L, "added", 5L);
            assertThat(cursor.getProperties()).isEqualTo(expected);
            assertThat(cursor.getReadPosition()).isEqualTo(readPosition);
            assertThat(cursor.getNumberOfEntriesInBacklog(false)).isEqualTo(2);
            if (retryReset) {
                // Rollover advances a cursor at the last entry of a closed ledger to the next ledger. A failed
                // compaction must leave both unacknowledged entries visible to rollover and to the next seek.
                config.setMaxEntriesPerLedger(3);
                ledger.rollCurrentLedgerIfFull();
                Position afterRollover = ledger.addEntry(new byte[] {3});
                assertThat(afterRollover.getLedgerId()).isGreaterThan(next.getLedgerId());
                ledger.maybeUpdateCursorBeforeTrimmingConsumedLedger().get(5, TimeUnit.SECONDS);
                assertThat(cursor.getMarkDeletedPosition()).isEqualTo(committed);
                resetCursor(cursor, committed).get(5, TimeUnit.SECONDS);
                assertThat(cursor.getMarkDeletedPosition()).isEqualTo(committed);
            } else {
                cursor.close();
            }
            ManagedCursorImpl recovered = new ManagedCursorImpl(bkc, ledger, cursorName);
            try {
                recoverCursor(recovered);
                assertThat(recovered.getMarkDeletedPosition()).isEqualTo(committed);
                assertThat(recovered.getProperties()).isEqualTo(expected);
            } finally {
                recovered.closeReadOnlyCursorLedger();
            }
        } finally {
            releaseRead.complete(null);
            failCheckpoint.completeExceptionally(failure);
            bkc.setReadHandleInterceptor(null);
        }
    }

    @Test(timeOut = 30000)
    public void failedResetReopensAckAdmission() throws Exception {
        String ledgerName = "failed-reset-ack";
        String cursorName = "sub";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(cursorName);
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        Position markDelete = positions.get(4);
        Position ackPosition = positions.get(6);
        cursor.markDelete(markDelete);

        String cursorPath = "/managed-ledgers/" + ledgerName + "/" + cursorName;
        metadataStore.failConditional(new MetadataStoreException("Failed reset checkpoint write"),
                (op, path) -> op == OperationType.PUT && path.equals(cursorPath));
        CompletableFuture<Void> failResetWrite = bkc.promiseAfter(0);
        BKException failure = BKException.create(BKException.Code.WriteException);
        try {
            Position target = positions.getLast();
            Position seek = AckSetStateUtil.createPositionWithAckSet(target.getLedgerId(), target.getEntryId(),
                    new long[] {0b10L});
            CompletableFuture<Void> reset = resetCursor(cursor, seek);
            ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(reset).isNotDone();

            AtomicInteger ackCallbacks = new AtomicInteger();
            CompletableFuture<Void> resetCompletion = rejectDeleteDuringReset(cursor, ackPosition, ackCallbacks)
                    .getResetCompletion().toCompletableFuture();
            assertThat(resetCompletion).isNotDone();

            failResetWrite.completeExceptionally(failure);
            assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(InvalidCursorPositionException.class);
            resetCompletion.get(5, TimeUnit.SECONDS);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(markDelete);
            assertThat(cursor.isMessageDeleted(ackPosition)).isFalse();
            cursor.delete(ackPosition);
            assertThat(cursor.isMessageDeleted(ackPosition)).isTrue();
            // The new cursor ledger must start from the last valid checkpoint, even after a forward seek fails.
            PositionInfo seed = readCursorCheckpoint(cursor, 0);
            assertThat(PositionFactory.create(seed.getLedgerId(), seed.getEntryId())).isEqualTo(markDelete);
            assertThat(seed.getBatchedEntryDeletionIndexInfosCount()).isZero();
            assertThat(ackCallbacks).hasValue(1);
        } finally {
            failResetWrite.completeExceptionally(failure);
        }
    }

    @DataProvider
    public Object[][] inlinePersistenceCompletion() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "inlinePersistenceCompletion", timeOut = 30000)
    public void ackCompletionKeepsPersistenceThreadUnlessWriteCompletesInline(boolean inline) throws Exception {
        ManagedLedger ledger = factory.open("ack-persistence-completion-thread", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position first = ledger.addEntry(new byte[] {0});
        Position second = ledger.addEntry(new byte[] {1});
        cursor.markDelete(first);

        LedgerHandle cursorLedger = cursor.cursorLedger;
        LedgerHandle storage = spy(cursorLedger);
        CompletableFuture<Runnable> completeWrite = new CompletableFuture<>();
        doAnswer(invocation -> {
            long entryId = cursorLedger.addEntry(invocation.getArgument(0, byte[].class));
            AddCallback callback = invocation.getArgument(1);
            Object ctx = invocation.getArgument(2);
            Runnable complete = () -> callback.addComplete(BKException.Code.OK, cursorLedger, entryId, ctx);
            if (inline) {
                complete.run();
            } else {
                completeWrite.complete(complete);
            }
            return null;
        }).when(storage).asyncAddEntry(any(byte[].class), any(AddCallback.class), any());

        cursor.cursorLedger = storage;
        try {
            Thread caller = Thread.currentThread();
            CompletableFuture<Thread> ack = new CompletableFuture<>();
            cursor.asyncDelete(second, new DeleteCallback() {
                @Override
                public void deleteComplete(Object ctx) {
                    ack.complete(Thread.currentThread());
                }

                @Override
                public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                    ack.completeExceptionally(exception);
                }
            }, null);

            if (inline) {
                assertThat(ack.get(5, TimeUnit.SECONDS)).isNotSameAs(caller);
            } else {
                assertThat(ack).isNotDone();
                completeWrite.get(5, TimeUnit.SECONDS).run();
                assertThat(ack.get(5, TimeUnit.SECONDS)).isSameAs(caller);
            }
            assertThat(cursor.getPersistentMarkDeletedPosition()).isEqualTo(second);
        } finally {
            cursor.cursorLedger = cursorLedger;
        }
    }

    @DataProvider
    public Object[][] cursorDeletion() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "cursorDeletion", timeOut = 30000)
    public void closeSettlesResetCompletionWhileReadIsPending(boolean deleteCursor) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("close-with-pending-reset", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        cursor.setInactive();
        Position first = ledger.addEntry(new byte[] {0});
        Position second = ledger.addEntry(new byte[] {1});
        Position third = ledger.addEntry(new byte[] {2});
        cursor.markDelete(first);

        CompletableFuture<Void> readStarted = new CompletableFuture<>();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
            readStarted.complete(null);
            return releaseRead.thenApply(__ -> entries);
        });
        CompletableFuture<Void> read = ManagedLedgerUtils.readEntries(cursor, 1, PositionFactory.LATEST)
                .thenAccept(entries -> entries.forEach(Entry::release));
        try {
            readStarted.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> earlierAck = ManagedLedgerUtils.markDelete(cursor, second, Map.of());
            assertThat(earlierAck).isNotDone();
            CompletableFuture<Void> reset = resetCursor(cursor, first);
            ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(reset).isNotDone();

            AtomicInteger ackCallbacks = new AtomicInteger();
            var resetCompletion = rejectDeleteDuringReset(cursor, third, ackCallbacks).getResetCompletion();
            assertThat(resetCompletion.toCompletableFuture()).isNotDone();

            if (deleteCursor) {
                CompletableFuture<Thread> settledOn = resetCompletion
                        .handle((__, error) -> Thread.currentThread()).toCompletableFuture();
                ledger.deleteCursor(cursor.getName());
                assertThat(settledOn.get(5, TimeUnit.SECONDS)).isNotSameAs(Thread.currentThread());
            } else {
                cursor.close();
            }
            assertThatThrownBy(() -> resetCompletion.toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CursorAlreadyClosedException.class);
            assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CursorAlreadyClosedException.class);
            assertThatThrownBy(() -> earlierAck.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CursorAlreadyClosedException.class);
            assertThat(releaseRead).isNotDone();
            assertThat(ackCallbacks).hasValue(1);
            if (!deleteCursor) {
                ManagedCursorInfo checkpoint = readCursorInfo(ledger, cursor.getName());
                assertThat(checkpoint.getCursorsLedgerId()).isEqualTo(-1L);
                Position storedMarkDelete = PositionFactory.create(checkpoint.getMarkDeleteLedgerId(),
                        checkpoint.getMarkDeleteEntryId());
                assertThat(storedMarkDelete).isEqualTo(first);
                assertRecoveredPosition(ledger, cursor.getName(), first, second);
            }
        } finally {
            releaseRead.complete(null);
            bkc.setReadHandleInterceptor(null);
            read.handle((__, error) -> null).get(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeOut = 30000)
    public void closePersistsSubmittedResetCheckpoint() throws Exception {
        // Keep both writes in the cursor ledger so their order is observable before recovery.
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("close-with-reset-write-in-flight",
                defaultConfig().setMaxUnackedRangesToPersistInMetadataStore(-1));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position first = ledger.addEntry(new byte[] {0});
        Position seekPosition = ledger.addEntry(new byte[] {1});
        Position markDelete = ledger.addEntry(new byte[] {2});
        ledger.addEntry(new byte[] {3});
        cursor.markDelete(markDelete);
        long lastCheckpoint = cursor.getCursorLedgerLastEntry();

        CompletableFuture<Void> releaseReset = bkc.addEntryResponseGate();
        CompletableFuture<Void> releaseClose = bkc.addEntryResponseGate();
        try {
            CompletableFuture<Void> reset = resetCursor(cursor, seekPosition);
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    cursor.getCursorLedgerLastEntry() == lastCheckpoint + 1
                            && releaseReset.getNumberOfDependents() > 0);
            PositionInfo resetCheckpoint = readCursorCheckpoint(cursor, lastCheckpoint + 1);
            assertThat(PositionFactory.create(resetCheckpoint.getLedgerId(), resetCheckpoint.getEntryId()))
                    .isEqualTo(first);
            assertThat(reset).isNotDone();

            CloseFuture close = new CloseFuture();
            cursor.asyncClose(close, null);
            ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(close).isNotDone();
            assertThat(reset).isNotDone();

            releaseReset.complete(null);
            reset.get(5, TimeUnit.SECONDS);
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    cursor.getCursorLedgerLastEntry() == lastCheckpoint + 2
                            && releaseClose.getNumberOfDependents() > 0);
            PositionInfo closeCheckpoint = readCursorCheckpoint(cursor, lastCheckpoint + 2);
            assertThat(PositionFactory.create(closeCheckpoint.getLedgerId(), closeCheckpoint.getEntryId()))
                    .isEqualTo(first);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(first);
            assertThat(cursor.getReadPosition()).isEqualTo(seekPosition);

            releaseClose.complete(null);
            close.get(5, TimeUnit.SECONDS);
            assertRecoveredPosition(ledger, cursor.getName(), first, seekPosition);
        } finally {
            releaseReset.complete(null);
            releaseClose.complete(null);
        }
    }

    @Test(timeOut = 30000)
    public void deleteCursorWhileCloseWaitsForSubmittedReset() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("delete-while-close-waits-for-reset",
                defaultConfig().setMaxUnackedRangesToPersistInMetadataStore(-1));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        ledger.addEntry(new byte[] {0});
        Position seekPosition = ledger.addEntry(new byte[] {1});
        Position priorMarkDelete = ledger.addEntry(new byte[] {2});
        ledger.addEntry(new byte[] {3});
        cursor.markDelete(priorMarkDelete);
        long lastCheckpoint = cursor.getCursorLedgerLastEntry();

        CompletableFuture<Void> releaseReset = bkc.addEntryResponseGate(cursor.getCursorLedger());
        try {
            CompletableFuture<Void> reset = resetCursor(cursor, seekPosition);
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    cursor.getCursorLedgerLastEntry() == lastCheckpoint + 1
                            && releaseReset.getNumberOfDependents() > 0);

            CloseFuture close = new CloseFuture();
            cursor.asyncClose(close, null);
            ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(close).isNotDone();
            assertThat(reset).isNotDone();

            ledger.deleteCursor(cursor.getName());
            assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CursorAlreadyClosedException.class);
            close.get(5, TimeUnit.SECONDS);

            assertThat(releaseReset).isNotDone();
            assertThat(cursor.getCursorLedgerLastEntry()).isEqualTo(lastCheckpoint + 1);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(priorMarkDelete);
            String cursorPath = "/managed-ledgers/" + ledger.getName() + "/" + cursor.getName();
            assertThat(metadataStore.get(cursorPath).get(5, TimeUnit.SECONDS)).isEmpty();
        } finally {
            releaseReset.complete(null);
        }
    }

    @Test(timeOut = 30000)
    public void closeWaitsForResetMetadataFallbackResponse() throws Exception {
        factory.shutdown();
        var storage = spy(metadataStore);
        factory = new ManagedLedgerFactoryImpl(storage, bkc);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("close-with-reset-metadata-fallback",
                defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position resetMarkDelete = ledger.addEntry(new byte[] {0});
        Position seekPosition = ledger.addEntry(new byte[] {1});
        cursor.markDelete(ledger.addEntry(new byte[] {2}));
        ledger.addEntry(new byte[] {3});

        String path = "/managed-ledgers/" + ledger.getName() + "/" + cursor.getName();
        CompletableFuture<Stat> fallbackStored = new CompletableFuture<>();
        CompletableFuture<Void> releaseFallback = new CompletableFuture<>();
        AtomicInteger metadataWrites = new AtomicInteger();
        doAnswer(invocation -> {
            int writeNumber = metadataWrites.incrementAndGet();
            Set<Option> options = invocation.getArgument(3);
            CompletableFuture<Stat> write = metadataStore.put(path, invocation.getArgument(1),
                    invocation.getArgument(2), options);
            if (writeNumber != 1) {
                return write;
            }
            return write.thenCompose(stat -> {
                fallbackStored.complete(stat);
                return releaseFallback.thenApply(ignored -> stat);
            });
        }).when(storage).put(eq(path), any(byte[].class), any(), anySet());

        try {
            bkc.addEntryFailAfter(0, BKException.Code.WriteException);
            CompletableFuture<Void> reset = resetCursor(cursor, seekPosition);
            Stat fallback = fallbackStored.get(5, TimeUnit.SECONDS);
            ManagedCursorInfo checkpoint = readCursorInfo(ledger, cursor.getName());
            assertThat(checkpoint.getCursorsLedgerId()).isEqualTo(-1L);
            assertThat(PositionFactory.create(checkpoint.getMarkDeleteLedgerId(), checkpoint.getMarkDeleteEntryId()))
                    .isEqualTo(resetMarkDelete);
            assertThat(reset).isNotDone();

            CloseFuture close = new CloseFuture();
            cursor.asyncClose(close, null);
            ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(close).isNotDone();
            assertThat(metadataWrites).hasValue(1);

            releaseFallback.complete(null);
            reset.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            assertThat(metadataWrites).hasValue(2);
            Stat closed = metadataStore.get(path).get(5, TimeUnit.SECONDS).orElseThrow().getStat();
            assertThat(closed.getVersion()).isEqualTo(fallback.getVersion() + 1);
            assertRecoveredPosition(ledger, cursor.getName(), resetMarkDelete, seekPosition);
        } finally {
            releaseFallback.complete(null);
        }
    }

    @Test(timeOut = 30000)
    public void closeWaitsForResetSubmittedAfterCursorLedgerSwitch() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("close-with-reset-after-ledger-switch",
                defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position resetMarkDelete = ledger.addEntry(new byte[] {0});
        Position seekPosition = ledger.addEntry(new byte[] {1});
        Position priorMarkDelete = ledger.addEntry(new byte[] {2});
        ledger.addEntry(new byte[] {3});
        cursor.markDelete(priorMarkDelete);

        Set<Long> existingLedgers = Set.copyOf(bkc.getLedgers());
        CompletableFuture<Void> releaseNewLedgerSeed = bkc.addEntryResponseGate();
        CompletableFuture<Void> reset;
        CloseFuture close = new CloseFuture();
        try {
            cursor.startCreatingNewMetadataLedger();
            await().atMost(5, TimeUnit.SECONDS).until(() -> releaseNewLedgerSeed.getNumberOfDependents() > 0);
            reset = resetCursor(cursor, seekPosition);
            ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(reset).isNotDone();

            // Arm the reset response gate before the seed response lets the switch submit the queued reset.
            List<Long> newLedgers = bkc.getLedgers().stream().filter(id -> !existingLedgers.contains(id)).toList();
            assertThat(newLedgers).hasSize(1);
            long newCursorLedger = newLedgers.getFirst();
            CompletableFuture<Void> releaseReset = bkc.addEntryResponseGate(newCursorLedger);
            try {
                releaseNewLedgerSeed.complete(null);
                await().atMost(5, TimeUnit.SECONDS).until(() -> releaseReset.getNumberOfDependents() > 0);
                assertThat(cursor.getCursorLedger()).isEqualTo(newCursorLedger);
                cursor.asyncClose(close, null);
                ledger.getExecutor().submit(() -> { }).get(5, TimeUnit.SECONDS);
                assertThat(cursor.getState()).isEqualTo(ManagedCursorImpl.State.Closing.toString());
                assertThat(close).isNotDone();
                assertThat(reset).isNotDone();
            } finally {
                releaseReset.complete(null);
            }
        } finally {
            releaseNewLedgerSeed.complete(null);
        }
        reset.get(5, TimeUnit.SECONDS);
        close.get(5, TimeUnit.SECONDS);
        ManagedCursorInfo checkpoint = readCursorInfo(ledger, cursor.getName());
        assertThat(checkpoint.getCursorsLedgerId()).isEqualTo(-1L);
        assertThat(PositionFactory.create(checkpoint.getMarkDeleteLedgerId(), checkpoint.getMarkDeleteEntryId()))
                .isEqualTo(resetMarkDelete);
        assertRecoveredPosition(ledger, cursor.getName(), resetMarkDelete, seekPosition);
    }

    private static void recoverCursor(ManagedCursorImpl recovered) throws Exception {
        CompletableFuture<Void> recovery = new CompletableFuture<>();
        recovered.recover(new VoidCallback() {
            @Override
            public void operationComplete() {
                recovery.complete(null);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                recovery.completeExceptionally(exception);
            }
        });
        recovery.get(5, TimeUnit.SECONDS);
    }

    private void assertRecoveredPosition(ManagedLedgerImpl ledger, String cursorName, Position markDelete,
            Position readPosition) throws Exception {
        ManagedCursorImpl recovered = new ManagedCursorImpl(bkc, ledger, cursorName);
        try {
            recoverCursor(recovered);
            assertThat(recovered.getMarkDeletedPosition()).isEqualTo(markDelete);
            assertThat(recovered.getReadPosition()).isEqualTo(readPosition);
        } finally {
            recovered.closeReadOnlyCursorLedger();
        }
    }

    private static CursorResetInProgressException rejectDeleteDuringReset(ManagedCursorImpl cursor, Position position,
            AtomicInteger ackCallbacks) {
        CompletableFuture<ManagedLedgerException> ack = new CompletableFuture<>();
        cursor.asyncDelete(position, new DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {
                ackCallbacks.incrementAndGet();
                ack.complete(null);
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                ackCallbacks.incrementAndGet();
                ack.complete(exception);
            }
        }, null);
        assertThat(ack).isDone();
        assertThat(ackCallbacks).hasValue(1);
        ManagedLedgerException result = ack.join();
        assertThat(result).isInstanceOf(CursorResetInProgressException.class);
        return (CursorResetInProgressException) result;
    }

    private static CompletableFuture<Void> resetCursor(ManagedCursorImpl cursor, Position position) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        cursor.asyncResetCursor(position, false, new ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                result.complete(null);
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        });
        return result;
    }

    private static ManagedCursorInfo readCursorInfo(ManagedLedgerImpl ledger, String cursorName) throws Exception {
        CompletableFuture<ManagedCursorInfo> result = new CompletableFuture<>();
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), cursorName, new MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedCursorInfo info, Stat stat) {
                result.complete(info);
            }

            @Override
            public void operationFailed(MetaStoreException exception) {
                result.completeExceptionally(exception);
            }
        });
        return result.get(5, TimeUnit.SECONDS);
    }

    private static PositionInfo readCursorCheckpoint(ManagedCursorImpl cursor) throws Exception {
        return readCursorCheckpoint(cursor, cursor.getCursorLedgerLastEntry());
    }

    private static PositionInfo readCursorCheckpoint(ManagedCursorImpl cursor, long entryId) throws Exception {
        CompletableFuture<PositionInfo> result = new CompletableFuture<>();
        cursor.cursorLedger.asyncReadEntries(entryId, entryId, (rc, lh, entries, ctx) -> {
            if (rc != BKException.Code.OK) {
                result.completeExceptionally(BKException.create(rc));
                return;
            }
            try {
                PositionInfo checkpoint = new PositionInfo();
                checkpoint.parseFrom(entries.nextElement().getEntry());
                result.complete(checkpoint);
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        }, null);
        return result.get(5, TimeUnit.SECONDS);
    }
}

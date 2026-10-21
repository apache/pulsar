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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ResetCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.MarkDeleteEntry;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class ManagedCursorResetCloseRaceTest extends MockedBookKeeperTestCase {

    private enum ResetState {
        CLEAR_ALL,
        REQUESTED_BATCH
    }

    @DataProvider(name = "closeAfterResetWrite")
    public Object[][] closeAfterResetWrite() {
        return new Object[][] {
                {ResetState.CLEAR_ALL, false, 20, false},
                {ResetState.REQUESTED_BATCH, false, -1, false},
                {ResetState.REQUESTED_BATCH, true, -1, true}
        };
    }

    @Test(dataProvider = "closeAfterResetWrite")
    public void testCloseWaitsForResetAndPersistsItsAcknowledgments(ResetState state, boolean backward,
                                                                  int metadataThreshold, boolean failCloseWrite)
            throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0)
                .setMaxUnackedRangesToPersistInMetadataStore(metadataThreshold);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        String name = "public/default/persistent/reset-close-" + state + "-" + backward
                + "-" + metadataThreshold + "-" + failCloseWrite;
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        CursorFixture fixture = prepareCursor(ledger, backward ? 5 : 0);
        ManagedCursorImpl original = fixture.cursor();
        List<Position> positions = fixture.positions();
        Position target = positions.get(2);
        original.delete(positions.get(8));
        deleteBatch(original, positions.get(9), 2L);
        if (state == ResetState.REQUESTED_BATCH && !backward) {
            deleteBatch(original, target, 4L);
        }
        original.markDelete(positions.get(backward ? 5 : 0));
        ManagedCursorImpl cursor = replaceWithSpy(ledger, original);
        Position oldMarkDelete = cursor.getMarkDeletedPosition();
        Position oldRead = cursor.getReadPosition();
        Position resetPosition = state == ResetState.REQUESTED_BATCH
                ? AckSetStateUtil.createPositionWithAckSet(target.getLedgerId(), target.getEntryId(), new long[] {6L})
                : target;
        ResetFuture reset = new ResetFuture();
        CloseFuture close = new CloseFuture();

        try (ResetCompletionGate gate = new ResetCompletionGate(cursor)) {
            cursor.asyncResetCursor(resetPosition, false, reset);
            gate.appendComplete.get(5, TimeUnit.SECONDS);
            assertThat(gate.persistFailure.get()).isNull();
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(oldMarkDelete);
            assertThat(cursor.getReadPosition()).isEqualTo(oldRead);
            AtomicInteger metadataWrites = observeCursorWrites(ledger);

            cursor.asyncClose(close, null);
            awaitExecutor(cursor);

            assertThat(close.isDone()).as("close must wait for reset alignment").isFalse();
            assertThat(cursor.getState()).as("close must leave reset able to finish").isEqualTo("Open");
            assertThat(metadataWrites.get()).as("close must not checkpoint the old ACK state").isZero();
            assertThat(gate.otherPersistCalls.get()).as("close must not append the old ACK state").isZero();
            if (failCloseWrite) {
                bkc.addEntryFailAfter(0, BKException.Code.NoBookieAvailableException);
            }

            gate.release();
            assertThat(reset.get(5, TimeUnit.SECONDS)).isEqualTo(resetPosition);
            close.get(5, TimeUnit.SECONDS);
            awaitExecutor(cursor);
        }

        ManagedCursorInfo authoritative = storedCursor(ledger);
        if (metadataThreshold < 0 && !failCloseWrite) {
            assertThat(authoritative.getCursorsLedgerId()).isEqualTo(cursor.getCursorLedger());
        } else {
            assertThat(authoritative.getCursorsLedgerId()).isEqualTo(-1L);
        }

        // The cursor is already closed, so closing its ledger cannot repair the persisted cursor state.
        ledger.close();
        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered.getMarkDeletedPosition()).isEqualTo(positions.get(1));
        assertThat(recovered.getReadPosition()).isEqualTo(target);
        assertThat(recovered.isMessageDeleted(positions.get(8))).isFalse();
        assertThat(recovered.getBatchPositionAckSet(positions.get(9))).isNull();
        if (state == ResetState.REQUESTED_BATCH) {
            assertThat(recovered.getBatchPositionAckSet(target)).containsExactly(6L);
        } else {
            assertThat(recovered.getBatchPositionAckSet(target)).isNull();
        }
    }

    @Test
    public void testCloseWaitingForFailedResetPreservesPreviousAcknowledgments() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0)
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        String name = "public/default/persistent/reset-failed-then-close";
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        CursorFixture fixture = prepareCursor(ledger, 0);
        ManagedCursorImpl original = fixture.cursor();
        List<Position> positions = fixture.positions();
        Position oldMarkDelete = positions.get(0);
        Position oldRead = positions.get(1);
        Position target = positions.get(3);
        Position individuallyAcknowledged = positions.get(8);
        original.delete(individuallyAcknowledged);
        deleteBatch(original, target, 4L);
        deleteBatch(original, positions.get(9), 2L);
        original.markDelete(oldMarkDelete);
        ManagedCursorImpl cursor = replaceWithSpy(ledger, original);
        ResetFuture reset = new ResetFuture();
        CloseFuture close = new CloseFuture();

        assertThat(ledger.getPreviousPosition(target)).isGreaterThan(oldMarkDelete);

        bkc.addEntryFailAfter(0, BKException.Code.NoBookieAvailableException);
        metadataStore.failConditional(new MetadataStoreException("reset metadata-store write failed"),
                (operation, path) -> operation == OperationType.PUT && path.equals(cursorPath(ledger)));
        try (ResetCompletionGate gate = new ResetCompletionGate(cursor)) {
            cursor.asyncResetCursor(AckSetStateUtil.createPositionWithAckSet(
                    target.getLedgerId(), target.getEntryId(), new long[] {6L}), false, reset);
            gate.appendComplete.get(5, TimeUnit.SECONDS);
            assertThat(gate.persistFailure.get()).isNotNull();
            AtomicInteger metadataWrites = observeCursorWrites(ledger);

            cursor.asyncClose(close, null);
            awaitExecutor(cursor);

            assertThat(close.isDone()).isFalse();
            assertThat(cursor.getState()).isEqualTo("NoLedger");
            assertThat(metadataWrites.get()).isZero();
            assertThat(gate.otherPersistCalls.get()).isZero();
            gate.release();
            assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(InvalidCursorPositionException.class);
            close.get(5, TimeUnit.SECONDS);
        }
        ManagedCursorInfo authoritative = storedCursor(ledger);
        assertThat(authoritative.getCursorsLedgerId()).isEqualTo(-1L);
        assertThat(authoritative.getMarkDeleteLedgerId()).isEqualTo(oldMarkDelete.getLedgerId());
        assertThat(authoritative.getMarkDeleteEntryId()).isEqualTo(oldMarkDelete.getEntryId());
        assertIndividualAck(authoritative, individuallyAcknowledged);
        assertThat(authoritative.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(2);
        assertBatchAck(authoritative, target, 4L);
        assertBatchAck(authoritative, positions.get(9), 2L);

        ledger.close();
        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered.getMarkDeletedPosition()).isEqualTo(oldMarkDelete);
        assertThat(recovered.getReadPosition()).isEqualTo(oldRead);
        assertThat(recovered.isMessageDeleted(individuallyAcknowledged)).isTrue();
        assertThat(recovered.getBatchPositionAckSet(target)).containsExactly(4L);
        assertThat(recovered.getBatchPositionAckSet(positions.get(9))).containsExactly(2L);
    }

    @Test
    public void testCloseWaitsForResetQueuedBehindPendingRead() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0)
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "public/default/persistent/reset-close-pending-read", config);
        CursorFixture fixture = prepareCursor(ledger, 0);
        ManagedCursorImpl cursor = fixture.cursor();
        List<Position> positions = fixture.positions();
        cursor.delete(positions.get(8));
        CompletableFuture<Void> readEntered = new CompletableFuture<>();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        CompletableFuture<Void> readComplete = new CompletableFuture<>();
        AtomicBoolean firstRead = new AtomicBoolean(true);
        ResetFuture reset = new ResetFuture();
        CloseFuture close = new CloseFuture();
        bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
            if (ledgerId == positions.get(0).getLedgerId() && firstRead.compareAndSet(true, false)) {
                readEntered.complete(null);
                return releaseRead.thenApply(ignored -> entries);
            }
            return CompletableFuture.completedFuture(entries);
        });
        try {
            cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entries.forEach(Entry::release);
                    readComplete.complete(null);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    readComplete.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);
            readEntered.get(5, TimeUnit.SECONDS);
            cursor.asyncResetCursor(positions.get(2), false, reset);
            awaitExecutor(cursor);
            assertThat(reset.isDone()).isFalse();
            AtomicInteger metadataWrites = observeCursorWrites(ledger);

            cursor.asyncClose(close, null);
            awaitExecutor(cursor);

            assertThat(close.isDone()).isFalse();
            assertThat(cursor.getState()).as("queued reset must remain able to flush when the read finishes")
                    .isEqualTo("Open");
            assertThat(metadataWrites.get()).isZero();
            releaseRead.complete(null);
            readComplete.get(5, TimeUnit.SECONDS);
            assertThat(reset.get(5, TimeUnit.SECONDS)).isEqualTo(positions.get(2));
            close.get(5, TimeUnit.SECONDS);
            ManagedCursorInfo authoritative = storedCursor(ledger);
            assertThat(authoritative.getCursorsLedgerId()).isEqualTo(-1L);
            assertThat(authoritative.getMarkDeleteEntryId()).isEqualTo(positions.get(1).getEntryId());
            assertThat(authoritative.getIndividualDeletedMessagesCount()).isZero();
        } finally {
            bkc.setReadHandleInterceptor(null);
            releaseRead.complete(null);
        }
    }

    @Test
    public void testResetCompletionCallbackCanCloseCursor() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0)
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "public/default/persistent/reset-callback-closes-cursor", config);
        CursorFixture fixture = prepareCursor(ledger, 5);
        ManagedCursorImpl cursor = fixture.cursor();
        List<Position> positions = fixture.positions();
        cursor.delete(positions.get(8));
        deleteBatch(cursor, positions.get(9), 2L);
        ResetFuture reset = new ResetFuture();
        CloseFuture close = new CloseFuture();
        Position target = positions.get(2);

        cursor.asyncResetCursor(AckSetStateUtil.createPositionWithAckSet(
                target.getLedgerId(), target.getEntryId(), new long[] {6L}), false, new ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                cursor.asyncClose(close, null);
                reset.resetComplete(ctx);
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                reset.resetFailed(exception, ctx);
            }
        });

        assertThat(reset.get(5, TimeUnit.SECONDS)).isEqualTo(target);
        close.get(5, TimeUnit.SECONDS);
        ManagedCursorInfo authoritative = storedCursor(ledger);
        assertThat(authoritative.getCursorsLedgerId()).isEqualTo(-1L);
        assertThat(authoritative.getMarkDeleteEntryId()).isEqualTo(positions.get(1).getEntryId());
        assertThat(authoritative.getIndividualDeletedMessagesCount()).isZero();
        assertRequestedBatch(authoritative, target);
    }

    @Test
    public void testResetCompletionPreservesConcurrentCompactionPropertyChanges() throws Exception {
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "public/default/persistent/reset-preserves-compaction-properties",
                defaultConfig().setThrottleMarkDelete(0));
        ManagedCursorImpl original = (ManagedCursorImpl) ledger.openCursor("__compaction");
        Position markDeletePosition = ledger.addEntry(new byte[] {0});
        Position resetPosition = ledger.addEntry(new byte[] {1});
        original.markDelete(markDeletePosition,
                Map.of("CompactedTopicLedger", 1L, "obsolete", 9L));
        ManagedCursorImpl cursor = replaceWithSpy(ledger, original);
        ResetFuture reset = new ResetFuture();
        Map<String, Long> expectedProperties = Map.of("CompactedTopicLedger", 2L);

        try (ResetCompletionGate gate = new ResetCompletionGate(cursor)) {
            cursor.asyncResetCursor(resetPosition, false, reset);
            gate.appendComplete.get(5, TimeUnit.SECONDS);

            assertThat(gate.persistFailure.get()).isNull();
            assertThat(reset.isDone()).isFalse();
            assertThat(cursor.putProperty("CompactedTopicLedger", 2L)).isTrue();
            assertThat(cursor.removeProperty("obsolete")).isTrue();
            assertThat(cursor.getProperties()).isEqualTo(expectedProperties);

            gate.release();
            assertThat(reset.get(5, TimeUnit.SECONDS)).isEqualTo(resetPosition);
        }

        assertThat(cursor.getProperties()).isEqualTo(expectedProperties);
    }

    @Test
    public void testResetPreservesPropertiesFromQueuedCompactionMarkDelete() throws Exception {
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "public/default/persistent/reset-preserves-queued-compaction-properties",
                defaultConfig().setThrottleMarkDelete(0));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("__compaction");
        cursor.setInactive();
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        CompletableFuture<Void> readEntered = new CompletableFuture<>();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        CompletableFuture<Void> readComplete = new CompletableFuture<>();
        CompletableFuture<Void> markDeleteComplete = new CompletableFuture<>();
        AtomicBoolean firstRead = new AtomicBoolean(true);
        ResetFuture reset = new ResetFuture();
        Map<String, Long> properties = Map.of("CompactedTopicLedger", 123L);
        bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
            if (ledgerId == positions.get(0).getLedgerId() && firstRead.compareAndSet(true, false)) {
                readEntered.complete(null);
                return releaseRead.thenApply(ignored -> entries);
            }
            return CompletableFuture.completedFuture(entries);
        });
        try {
            cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entries.forEach(Entry::release);
                    readComplete.complete(null);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    readComplete.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);
            readEntered.get(5, TimeUnit.SECONDS);
            cursor.asyncMarkDelete(positions.get(0), properties, new MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    markDeleteComplete.complete(null);
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                    markDeleteComplete.completeExceptionally(exception);
                }
            }, null);
            cursor.asyncResetCursor(positions.get(1), false, reset);
            awaitExecutor(cursor);

            assertThat(markDeleteComplete.isDone()).isFalse();
            assertThat(reset.isDone()).isFalse();
            releaseRead.complete(null);
            readComplete.get(5, TimeUnit.SECONDS);
            markDeleteComplete.get(5, TimeUnit.SECONDS);
            assertThat(reset.get(5, TimeUnit.SECONDS)).isEqualTo(positions.get(1));
            assertThat(cursor.getProperties()).isEqualTo(properties);
        } finally {
            bkc.setReadHandleInterceptor(null);
            releaseRead.complete(null);
        }
    }

    private static CursorFixture prepareCursor(ManagedLedgerImpl ledger, int markDeleteIndex) throws Exception {
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        cursor.setInactive();
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        cursor.markDelete(positions.get(markDeleteIndex));
        return new CursorFixture(cursor, positions);
    }

    private record CursorFixture(ManagedCursorImpl cursor, List<Position> positions) {
    }

    private static ManagedCursorImpl replaceWithSpy(ManagedLedgerImpl ledger, ManagedCursorImpl original) {
        ManagedCursorImpl cursor = spy(original);
        ledger.getCursors().removeCursor(original.getName());
        ledger.getCursors().add(cursor, null);
        return cursor;
    }

    private static void deleteBatch(ManagedCursorImpl cursor, Position position, long words) throws Exception {
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(position.getLedgerId(), position.getEntryId(),
                new long[] {words}));
    }

    private static void assertRequestedBatch(ManagedCursorInfo cursorInfo, Position target) {
        assertThat(cursorInfo.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(1);
        assertBatchAck(cursorInfo, target, 6L);
    }

    private static void assertBatchAck(ManagedCursorInfo cursorInfo, Position target, long expectedAckSet) {
        BatchedEntryDeletionIndexInfo batch = cursorInfo.getBatchedEntryDeletionIndexInfosList().stream()
                .filter(entry -> entry.getPosition().getLedgerId() == target.getLedgerId()
                        && entry.getPosition().getEntryId() == target.getEntryId())
                .findFirst()
                .orElseThrow();
        assertThat(batch.getPosition().getLedgerId()).isEqualTo(target.getLedgerId());
        assertThat(batch.getPosition().getEntryId()).isEqualTo(target.getEntryId());
        assertThat(batch.getDeleteSetsCount()).isEqualTo(1);
        assertThat(batch.getDeleteSetAt(0)).isEqualTo(expectedAckSet);
    }

    private static void assertIndividualAck(ManagedCursorInfo cursorInfo, Position acknowledgedPosition) {
        assertThat(cursorInfo.getIndividualDeletedMessagesCount()).isEqualTo(1);
        MessageRange range = cursorInfo.getIndividualDeletedMessageAt(0);
        assertThat(range.getLowerEndpoint().getLedgerId()).isEqualTo(acknowledgedPosition.getLedgerId());
        assertThat(range.getLowerEndpoint().getEntryId()).isEqualTo(acknowledgedPosition.getEntryId() - 1);
        assertThat(range.getUpperEndpoint().getLedgerId()).isEqualTo(acknowledgedPosition.getLedgerId());
        assertThat(range.getUpperEndpoint().getEntryId()).isEqualTo(acknowledgedPosition.getEntryId());
    }

    private AtomicInteger observeCursorWrites(ManagedLedgerImpl ledger) {
        AtomicInteger writes = new AtomicInteger();
        metadataStore.failConditional(new MetadataStoreException("unused cursor-write observer"), (operation, path) -> {
            if (operation == OperationType.PUT && path.equals(cursorPath(ledger))) {
                writes.incrementAndGet();
            }
            return false;
        });
        return writes;
    }

    private static String cursorPath(ManagedLedgerImpl ledger) {
        return "/managed-ledgers/" + ledger.getName() + "/sub";
    }

    private static void awaitExecutor(ManagedCursorImpl cursor) throws Exception {
        CompletableFuture<Void> barrier = new CompletableFuture<>();
        cursor.ledger.getExecutor().execute(() -> barrier.complete(null));
        barrier.get(5, TimeUnit.SECONDS);
    }

    private static ManagedCursorInfo storedCursor(ManagedLedgerImpl ledger) throws Exception {
        CompletableFuture<ManagedCursorInfo> result = new CompletableFuture<>();
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), "sub", new MetaStoreCallback<>() {
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

    private static class ResetFuture extends CompletableFuture<Position> implements ResetCursorCallback {
        @Override
        public void resetComplete(Object ctx) {
            complete((Position) ctx);
        }

        @Override
        public void resetFailed(ManagedLedgerException exception, Object ctx) {
            completeExceptionally(exception);
        }
    }

    private static class ResetCompletionGate implements AutoCloseable {
        private final ManagedCursorImpl cursor;
        private final CompletableFuture<Void> appendComplete = new CompletableFuture<>();
        private final AtomicInteger otherPersistCalls = new AtomicInteger();
        private final AtomicReference<Runnable> heldCompletion = new AtomicReference<>();
        private final AtomicReference<ManagedLedgerException> persistFailure = new AtomicReference<>();

        private ResetCompletionGate(ManagedCursorImpl cursor) {
            this.cursor = cursor;
            AtomicBoolean firstPersist = new AtomicBoolean(true);
            AtomicBoolean delegatingReset = new AtomicBoolean();
            doAnswer(invocation -> {
                if (!firstPersist.compareAndSet(true, false)) {
                    if (!delegatingReset.get()) {
                        otherPersistCalls.incrementAndGet();
                    }
                    return invocation.callRealMethod();
                }
                VoidCallback callback = invocation.getArgument(2);
                delegatingReset.set(true);
                try {
                    cursor.persistPositionToLedger(invocation.getArgument(0), invocation.getArgument(1),
                            new VoidCallback() {
                                @Override
                                public void operationComplete() {
                                    heldCompletion.set(callback::operationComplete);
                                    appendComplete.complete(null);
                                }

                                @Override
                                public void operationFailed(ManagedLedgerException exception) {
                                    persistFailure.set(exception);
                                    heldCompletion.set(() -> callback.operationFailed(exception));
                                    appendComplete.complete(null);
                                }
                            }, invocation.getArgument(3));
                } finally {
                    delegatingReset.set(false);
                }
                return null;
            }).when(cursor).persistPositionToLedger(any(LedgerHandle.class), any(MarkDeleteEntry.class),
                    any(VoidCallback.class), anyBoolean());
        }

        private void release() throws Exception {
            Runnable completion = heldCompletion.getAndSet(null);
            if (completion == null) {
                return;
            }
            CompletableFuture<Void> resumed = new CompletableFuture<>();
            cursor.ledger.getExecutor().execute(() -> {
                try {
                    completion.run();
                    resumed.complete(null);
                } catch (Throwable exception) {
                    resumed.completeExceptionally(exception);
                }
            });
            resumed.get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            release();
        }
    }

}

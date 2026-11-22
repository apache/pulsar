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
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ResetCursorCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.CursorAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
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

            List<Position> ackPositions = new ArrayList<>(List.of(ackPosition));
            CompletableFuture<Void> ack = deleteAsync(cursor, ackPositions, ackCallbacks);
            ackPositions.clear();
            assertThat(ack).isNotDone();
            assertThat(cursor.isMessageDeleted(ackPosition)).isFalse();

            releaseReset.complete(null);
            reset.get(5, TimeUnit.SECONDS);
            releaseRead.complete(null);
            read.get(5, TimeUnit.SECONDS);
            ack.get(5, TimeUnit.SECONDS);

            // Check the record recovery would read before another write (including close) can replace it.
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(resetMarkDelete);
            assertThat(cursor.isMessageDeleted(positions.get(2))).isFalse();
            assertThat(cursor.isMessageDeleted(ackPosition)).isTrue();
            assertPersistedAck(cursor, resetMarkDelete, ackPosition);
            assertThat(ackCallbacks).hasValue(1);
        } finally {
            releaseReset.complete(null);
            releaseRead.complete(null);
            bkc.setReadHandleInterceptor(null);
        }
    }

    @Test(timeOut = 30000)
    public void individualAckDuringFailedDurableResetUsesCurrentPosition() throws Exception {
        String ledgerName = "failed-reset-ack";
        String cursorName = "sub";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName,
                defaultConfig().setPersistIndividualAckAsLongArray(false));
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
            CompletableFuture<Void> reset = resetCursor(cursor, positions.get(2));
            // The ordered reset task has submitted its add, which is waiting on the programmed failure.
            CompletableFuture.runAsync(() -> { }, ledger.getExecutor()).get(5, TimeUnit.SECONDS);
            assertThat(reset).isNotDone();

            AtomicInteger ackCallbacks = new AtomicInteger();
            CompletableFuture<Void> ack = deleteAsync(cursor, List.of(ackPosition), ackCallbacks);
            assertThat(ack).isNotDone();
            assertThat(cursor.isMessageDeleted(ackPosition)).isFalse();

            failResetWrite.completeExceptionally(failure);
            assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(InvalidCursorPositionException.class);
            ack.get(5, TimeUnit.SECONDS);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(markDelete);
            assertThat(cursor.isMessageDeleted(ackPosition)).isTrue();
            assertPersistedAck(cursor, markDelete, ackPosition);
            assertThat(ackCallbacks).hasValue(1);
        } finally {
            failResetWrite.completeExceptionally(failure);
        }
    }

    @Test(timeOut = 30000)
    public void closingCursorCompletesDeferredIndividualAck() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("close-with-deferred-ack", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        cursor.setInactive();
        Position first = ledger.addEntry(new byte[] {0});
        Position second = ledger.addEntry(new byte[] {1});
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
            CompletableFuture<Void> reset = resetCursor(cursor, first);
            CompletableFuture.runAsync(() -> { }, ledger.getExecutor()).get(5, TimeUnit.SECONDS);
            assertThat(reset).isNotDone();

            AtomicInteger ackCallbacks = new AtomicInteger();
            CompletableFuture<Void> ack = deleteAsync(cursor, List.of(second), ackCallbacks);
            assertThat(ack).isNotDone();
            cursor.close();

            assertThatThrownBy(() -> ack.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CursorAlreadyClosedException.class);
            assertThat(ackCallbacks).hasValue(1);
        } finally {
            releaseRead.complete(null);
            bkc.setReadHandleInterceptor(null);
            read.handle((__, error) -> null).get(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeOut = 30000)
    public void closeAfterResumedAckHasJoinedTheMarkDeleteQueue() throws Exception {
        ManagedLedger ledger = factory.open("close-after-resumed-ack", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        cursor.setInactive();
        Position first = ledger.addEntry(new byte[] {0});
        ledger.addEntry(new byte[] {1});
        Position third = ledger.addEntry(new byte[] {2});
        cursor.markDelete(first);

        long lastEntry = cursor.getCursorLedgerLastEntry();
        CompletableFuture<Void> releaseReset = bkc.addEntryResponseGate();
        CompletableFuture<Void> releaseRead = new CompletableFuture<>();
        try {
            CompletableFuture<Void> reset = resetCursor(cursor, first);
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    cursor.getCursorLedgerLastEntry() > lastEntry && releaseReset.getNumberOfDependents() > 0);
            CompletableFuture<Void> readStarted = new CompletableFuture<>();
            bkc.setReadHandleInterceptor((ledgerId, firstEntry, last, entries) -> {
                readStarted.complete(null);
                return releaseRead.thenApply(__ -> entries);
            });
            CompletableFuture<Void> read = ManagedLedgerUtils.readEntries(cursor, 1, PositionFactory.LATEST)
                    .thenAccept(entries -> entries.forEach(Entry::release));
            readStarted.get(5, TimeUnit.SECONDS);
            AtomicInteger ackCallbacks = new AtomicInteger();
            CompletableFuture<Void> ack = deleteAsync(cursor, List.of(third), ackCallbacks);
            assertThat(ack).isNotDone();

            releaseReset.complete(null);
            reset.get(5, TimeUnit.SECONDS);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                synchronized (cursor.pendingMarkDeleteOps) {
                    return cursor.pendingMarkDeleteOps.size() == 1;
                }
            });
            cursor.close();
            releaseRead.complete(null);
            read.handle((__, error) -> null).get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> ack.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CursorAlreadyClosedException.class);
            assertThat(ackCallbacks).hasValue(1);
        } finally {
            releaseReset.complete(null);
            releaseRead.complete(null);
            bkc.setReadHandleInterceptor(null);
        }
    }

    @Test(timeOut = 30000)
    public void resetWaitsForAnAlreadyAdmittedIndividualAck() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("ack-before-reset", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position first = ledger.addEntry(new byte[] {0});
        Position second = ledger.addEntry(new byte[] {1});
        Position third = ledger.addEntry(new byte[] {2});
        cursor.markDelete(second);

        Thread resetThread = CompletableFuture.supplyAsync(Thread::currentThread, ledger.getExecutor())
                .get(5, TimeUnit.SECONDS);
        CompletableFuture<Void> ackAdmitted = new CompletableFuture<>();
        CompletableFuture<Void> releaseAck = new CompletableFuture<>();
        Iterable<Position> positions = () -> {
            ackAdmitted.complete(null);
            releaseAck.join();
            return List.of(third).iterator();
        };
        AtomicInteger ackCallbacks = new AtomicInteger();
        try {
            CompletableFuture<Void> ack = CompletableFuture.completedFuture(positions)
                    .thenComposeAsync(ackPositions -> deleteAsync(cursor, ackPositions, ackCallbacks));
            ackAdmitted.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> reset = resetCursor(cursor, second);
            var threads = ManagementFactory.getThreadMXBean();
            int queueLock = System.identityHashCode(cursor.pendingMarkDeleteOps);
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                var resetThreadInfo = threads.getThreadInfo(resetThread.threadId());
                return resetThreadInfo != null && resetThreadInfo.getThreadState() == Thread.State.BLOCKED
                        && resetThreadInfo.getLockInfo() != null
                        && resetThreadInfo.getLockInfo().getIdentityHashCode() == queueLock;
            });

            releaseAck.complete(null);
            ack.get(5, TimeUnit.SECONDS);
            reset.get(5, TimeUnit.SECONDS);
            PositionInfo checkpoint = readCursorCheckpoint(cursor);
            assertThat(PositionFactory.create(checkpoint.getLedgerId(), checkpoint.getEntryId())).isEqualTo(first);
            assertThat(ackCallbacks).hasValue(1);
        } finally {
            releaseAck.complete(null);
        }
    }

    @Test
    public void nonDurableCumulativeAckDoesNotReportFailureAfterCallingSuccess() throws Exception {
        ManagedLedger ledger = factory.open("non-durable-ack-callback", defaultConfig());
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        Position position = ledger.addEntry(new byte[] {0});
        RuntimeException callbackError = new IllegalStateException("failed in success callback");
        AtomicInteger failureCallbacks = new AtomicInteger();
        assertThatThrownBy(() -> cursor.asyncMarkDelete(position, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                throw callbackError;
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                failureCallbacks.incrementAndGet();
            }
        }, null)).isSameAs(callbackError);
        assertThat(failureCallbacks).hasValue(0);
    }

    private static CompletableFuture<Void> deleteAsync(ManagedCursorImpl cursor, Iterable<Position> positions,
                                                      AtomicInteger callbacks) {
        CompletableFuture<Void> ack = new CompletableFuture<>();
        cursor.asyncDelete(positions, new DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {
                callbacks.incrementAndGet();
                ack.complete(null);
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                callbacks.incrementAndGet();
                ack.completeExceptionally(exception);
            }
        }, null);
        return ack;
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

    private static void assertPersistedAck(ManagedCursorImpl cursor, Position markDelete,
                                           Position ackPosition) throws Exception {
        PositionInfo checkpoint = readCursorCheckpoint(cursor);
        assertThat(PositionFactory.create(checkpoint.getLedgerId(), checkpoint.getEntryId())).isEqualTo(markDelete);
        assertThat(checkpoint.getIndividualDeletedMessagesCount()).isEqualTo(1);
        MessageRange ackRange = checkpoint.getIndividualDeletedMessageAt(0);
        assertThat(PositionFactory.create(ackRange.getLowerEndpoint().getLedgerId(),
                ackRange.getLowerEndpoint().getEntryId())).isEqualTo(
                        PositionFactory.create(ackPosition.getLedgerId(), ackPosition.getEntryId() - 1));
        assertThat(PositionFactory.create(ackRange.getUpperEndpoint().getLedgerId(),
                ackRange.getUpperEndpoint().getEntryId())).isEqualTo(ackPosition);
    }

    private static PositionInfo readCursorCheckpoint(ManagedCursorImpl cursor) throws Exception {
        CompletableFuture<PositionInfo> result = new CompletableFuture<>();
        long entryId = cursor.getCursorLedgerLastEntry();
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

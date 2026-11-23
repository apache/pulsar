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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class ManagedCursorCloseDuringLedgerSwitchTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 30000)
    public void testCloseWaitsForLedgerSwitchAndQueuedMarkDelete() throws Exception {
        String ledgerName = "close-during-cursor-ledger-switch";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.addEntry("first".getBytes());
        Position acknowledged = ledger.addEntry("acknowledged".getBytes());

        // Hold the add that initializes the new cursor ledger. The cursor stays in SwitchingLedger until released.
        CompletableFuture<Void> releaseLedgerSwitch = bkc.addEntryResponseGate();
        CompletableFuture<Void> releaseQueuedMarkDelete = bkc.addEntryResponseGate();
        CompletableFuture<Void> markDeleteFuture = markDelete(cursor, acknowledged);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> cursor.state == ManagedCursorImpl.State.SwitchingLedger);

        CompletableFuture<Void> closeFuture = close(cursor);

        boolean closeCompletedDuringSwitch;
        boolean closeCompletedWithQueuedMarkDelete;
        try {
            closeCompletedDuringSwitch = completesWithinOneSecond(closeFuture);
            releaseLedgerSwitch.complete(null);
            closeCompletedWithQueuedMarkDelete = completesWithinOneSecond(closeFuture);
        } finally {
            releaseLedgerSwitch.complete(null);
            releaseQueuedMarkDelete.complete(null);
        }

        assertThat(closeCompletedDuringSwitch)
                .as("close must wait for the cursor-ledger switch")
                .isFalse();
        assertThat(closeCompletedWithQueuedMarkDelete)
                .as("close must wait for the mark-delete queued behind the switch")
                .isFalse();
        markDeleteFuture.get(10, TimeUnit.SECONDS);
        closeFuture.get(10, TimeUnit.SECONDS);

        ManagedCursorInfo cursorInfo = readCursorInfo(ledger, cursor);
        assertThat(cursorInfo.getMarkDeleteLedgerId()).isEqualTo(acknowledged.getLedgerId());
        assertThat(cursorInfo.getMarkDeleteEntryId()).isEqualTo(acknowledged.getEntryId());
        assertThat(cursorInfo.getCursorsLedgerId()).isEqualTo(-1);
        ledger.close();
    }

    @Test(timeOut = 30000)
    public void testClosePersistsCompletedThrottledAckAfterLedgerCreationFailure() throws Exception {
        String ledgerName = "close-after-cursor-ledger-create-failure";
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0.001);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry("first".getBytes());
        Position acknowledged = ledger.addEntry("acknowledged".getBytes());

        CompletableFuture<Void> createGate = bkc.promiseAfter(0);
        CompletableFuture<Void> firstMarkDelete = markDelete(cursor, first);
        Awaitility.await().until(() -> cursor.state == ManagedCursorImpl.State.SwitchingLedger);

        // The first call consumes the limiter permit. This acknowledgement completes in memory while creation waits.
        markDelete(cursor, acknowledged).get(10, TimeUnit.SECONDS);
        CompletableFuture<Void> closeFuture = close(cursor);
        createGate.completeExceptionally(BKException.create(BKException.Code.NoBookieAvailableException));

        closeFuture.get(10, TimeUnit.SECONDS);
        assertThat(firstMarkDelete.handle((ignored, error) -> error).get(10, TimeUnit.SECONDS)).isNotNull();
        assertCursorInfoPosition(readCursorInfo(ledger, cursor), acknowledged);
        ledger.close();
    }

    @Test(timeOut = 30000)
    public void testCloseWaitsForMetadataFallbackAfterLedgerCreationFailure() throws Exception {
        String ledgerName = "close-during-cursor-metadata-fallback";
        String cursorName = "cursor";
        String cursorPath = "/managed-ledgers/" + ledgerName + "/" + cursorName;
        CompletableFuture<Void> metadataGate = new CompletableFuture<>();
        CountDownLatch metadataWriteStarted = new CountDownLatch(1);
        AtomicBoolean interceptNextCursorPut = new AtomicBoolean();
        FaultInjectionMetadataStore gatedStore = spy(metadataStore);
        doAnswer(invocation -> {
            if (!interceptNextCursorPut.compareAndSet(true, false)) {
                return invocation.callRealMethod();
            }
            byte[] value = invocation.getArgument(1);
            Optional<Long> expectedVersion = invocation.getArgument(2);
            Set<Option> options = invocation.getArgument(3);
            metadataWriteStarted.countDown();
            return metadataGate.thenCompose(__ -> metadataStore.put(cursorPath, value, expectedVersion, options));
        }).when(gatedStore).put(eq(cursorPath), any(byte[].class), any(),
                org.mockito.ArgumentMatchers.<Set<Option>>any());

        ManagedLedgerFactoryImpl gatedFactory = new ManagedLedgerFactoryImpl(gatedStore, bkc);
        try {
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) gatedFactory.open(ledgerName, defaultConfig());
            ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(cursorName);
            Position acknowledged = ledger.addEntry("acknowledged".getBytes());
            CompletableFuture<Void> createGate = bkc.promiseAfter(0);
            interceptNextCursorPut.set(true);
            CompletableFuture<Void> markDeleteFuture = markDelete(cursor, acknowledged);
            Awaitility.await().until(() -> cursor.state == ManagedCursorImpl.State.SwitchingLedger);
            createGate.completeExceptionally(BKException.create(BKException.Code.NoBookieAvailableException));
            assertThat(metadataWriteStarted.await(10, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<Void> closeFuture = close(cursor);
            assertThat(completesWithinOneSecond(closeFuture)).isFalse();
            metadataGate.complete(null);

            markDeleteFuture.get(10, TimeUnit.SECONDS);
            closeFuture.get(10, TimeUnit.SECONDS);
            assertCursorInfoPosition(readCursorInfo(ledger, cursor), acknowledged);
            ledger.close();
        } finally {
            metadataGate.complete(null);
            gatedFactory.shutdown();
        }
    }

    @Test(timeOut = 30000)
    public void testClosePersistsFinalPositionAfterQueuedMarkDeleteFailure() throws Exception {
        String ledgerName = "close-after-queued-mark-delete-failure";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position acknowledged = ledger.addEntry("acknowledged".getBytes());

        CompletableFuture<Void> releaseLedgerSwitch = bkc.addEntryResponseGate();
        bkc.addEntryFailAfter(1, BKException.Code.WriteException);
        CompletableFuture<Void> markDeleteFuture = markDelete(cursor, acknowledged);
        Awaitility.await().until(() -> cursor.state == ManagedCursorImpl.State.SwitchingLedger);
        CompletableFuture<Void> closeFuture = close(cursor);
        releaseLedgerSwitch.complete(null);

        assertThat(markDeleteFuture.handle((ignored, error) -> error).get(10, TimeUnit.SECONDS)).isNotNull();
        closeFuture.get(10, TimeUnit.SECONDS);
        assertCursorInfoPosition(readCursorInfo(ledger, cursor), acknowledged);
        ledger.close();
    }

    @Test(timeOut = 30000)
    public void testCloseWaitsForReplacementSwitchStartedByFailureCallback() throws Exception {
        String ledgerName = "close-during-reentrant-cursor-ledger-switch";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry("first".getBytes());
        Position acknowledged = ledger.addEntry("acknowledged".getBytes());

        CompletableFuture<Void> firstCreateGate = bkc.promiseAfter(0);
        CompletableFuture<Void> secondCreateGate = bkc.promiseAfter(1);
        CompletableFuture<Void> firstMarkDelete = new CompletableFuture<>();
        CompletableFuture<Void> retriedMarkDelete = new CompletableFuture<>();
        CompletableFuture<Void> retrySubmitted = new CompletableFuture<>();
        cursor.asyncMarkDelete(first, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                firstMarkDelete.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                firstMarkDelete.completeExceptionally(exception);
                cursor.asyncMarkDelete(acknowledged, new MarkDeleteCallback() {
                    @Override
                    public void markDeleteComplete(Object ctx) {
                        retriedMarkDelete.complete(null);
                    }

                    @Override
                    public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                        retriedMarkDelete.completeExceptionally(exception);
                    }
                }, null);
                retrySubmitted.complete(null);
            }
        }, null);
        Awaitility.await().until(() -> cursor.state == ManagedCursorImpl.State.SwitchingLedger);

        firstCreateGate.completeExceptionally(BKException.create(BKException.Code.NoBookieAvailableException));
        retrySubmitted.get(10, TimeUnit.SECONDS);
        CompletableFuture<Void> closeFuture = close(cursor);

        boolean closeCompletedBeforeReplacementSwitch;
        try {
            closeCompletedBeforeReplacementSwitch = completesWithinOneSecond(closeFuture);
        } finally {
            secondCreateGate.complete(null);
        }

        assertThat(closeCompletedBeforeReplacementSwitch)
                .as("close must wait for the replacement cursor-ledger switch")
                .isFalse();
        assertThat(firstMarkDelete.handle((ignored, error) -> error).get(10, TimeUnit.SECONDS)).isNotNull();
        retriedMarkDelete.get(10, TimeUnit.SECONDS);
        closeFuture.get(10, TimeUnit.SECONDS);
        assertCursorInfoPosition(readCursorInfo(ledger, cursor), acknowledged);
        ledger.close();
    }

    @Test(timeOut = 30000)
    public void testCloseDoesNotPersistFailedForwardReset() throws Exception {
        String ledgerName = "close-after-failed-forward-reset";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position acknowledged = ledger.addEntry("acknowledged".getBytes());
        ledger.addEntry("between".getBytes());
        Position resetReadPosition = ledger.addEntry("reset-read-position".getBytes());
        markDelete(cursor, acknowledged).get(10, TimeUnit.SECONDS);

        CompletableFuture<Void> releaseLedgerSwitch = bkc.addEntryResponseGate();
        bkc.addEntryFailAfter(1, BKException.Code.WriteException);
        cursor.startCreatingNewMetadataLedger();
        Awaitility.await().until(() -> cursor.state == ManagedCursorImpl.State.SwitchingLedger);
        CompletableFuture<Void> resetFuture = resetCursor(cursor, resetReadPosition);
        Awaitility.await().until(() -> cursor.pendingMarkDeleteOps.size() == 1);
        CompletableFuture<Void> closeFuture = close(cursor);
        releaseLedgerSwitch.complete(null);

        assertThat(resetFuture.handle((ignored, error) -> error).get(10, TimeUnit.SECONDS)).isNotNull();
        closeFuture.get(10, TimeUnit.SECONDS);
        ledger.close();

        ManagedLedgerImpl reopenedLedger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl reopenedCursor = (ManagedCursorImpl) reopenedLedger.openCursor("cursor");
        assertThat(reopenedCursor.getMarkDeletedPosition()).isEqualTo(acknowledged);
        reopenedLedger.close();
    }

    @Test(timeOut = 30000)
    public void testClosePersistsSuccessfulForwardResetWhileWriteIsInProgress() throws Exception {
        String ledgerName = "close-during-successful-forward-reset";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position acknowledged = ledger.addEntry("acknowledged".getBytes());
        Position expectedMarkDeletePosition = ledger.addEntry("between".getBytes());
        Position resetReadPosition = ledger.addEntry("reset-read-position".getBytes());
        markDelete(cursor, acknowledged).get(10, TimeUnit.SECONDS);

        CompletableFuture<Void> resetWriteGate = bkc.addEntryResponseGate();
        CompletableFuture<Void> resetFuture = resetCursor(cursor, resetReadPosition);
        Awaitility.await().until(() -> expectedMarkDeletePosition.equals(cursor.inProgressMarkDeletePersistPosition));

        CompletableFuture<Void> closeFuture = close(cursor);
        closeFuture.get(10, TimeUnit.SECONDS);
        resetWriteGate.complete(null);
        resetFuture.get(10, TimeUnit.SECONDS);
        ledger.close();

        ManagedLedgerImpl reopenedLedger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl reopenedCursor = (ManagedCursorImpl) reopenedLedger.openCursor("cursor");
        assertThat(reopenedCursor.getMarkDeletedPosition()).isEqualTo(expectedMarkDeletePosition);
        reopenedLedger.close();
    }

    @Test(timeOut = 30000)
    public void testCloseSettlesWhenCursorLedgerCreationTimesOutAndCompletesLate() throws Exception {
        String ledgerName = "close-after-cursor-ledger-create-timeout";
        ManagedLedgerConfig config = defaultConfig().setMetadataOperationsTimeoutSeconds(1);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position acknowledged = ledger.addEntry("acknowledged".getBytes());
        Set<Long> ledgersBeforeCreate = new HashSet<>(bkc.getLedgers());

        CompletableFuture<Void> createGate = bkc.promiseAfter(0);
        CompletableFuture<Void> markDeleteFuture = markDelete(cursor, acknowledged);
        Awaitility.await().until(() -> cursor.state == ManagedCursorImpl.State.SwitchingLedger);
        CompletableFuture<Void> closeFuture = close(cursor);

        closeFuture.get(10, TimeUnit.SECONDS);
        assertThat(markDeleteFuture.handle((ignored, error) -> error).get(10, TimeUnit.SECONDS)).isNotNull();
        CompletableFuture<Void> deleteGate = bkc.promiseAfter(0);
        createGate.complete(null);
        try {
            Awaitility.await().until(() -> bkc.getLedgers().size() > ledgersBeforeCreate.size());
        } finally {
            deleteGate.complete(null);
        }
        Awaitility.await().untilAsserted(() -> assertThat(bkc.getLedgers()).containsExactlyInAnyOrderElementsOf(
                ledgersBeforeCreate));
        assertThat(readCursorInfo(ledger, cursor).getCursorsLedgerId()).isEqualTo(-1);
        ledger.close();
    }

    @Test(timeOut = 30000)
    public void testDeletingCursorDoesNotStartDeferredLedgerCreation() throws Exception {
        String ledgerName = "delete-during-deferred-cursor-ledger-switch";
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(ledgerName, defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry("first".getBytes());
        Position second = ledger.addEntry("second".getBytes());
        Position third = ledger.addEntry("third".getBytes());
        markDelete(cursor, first).get(10, TimeUnit.SECONDS);
        Set<Long> ledgersBeforeSwitch = new HashSet<>(bkc.getLedgers());

        CompletableFuture<Void> oldLedgerWriteGate = bkc.addEntryResponseGate();
        CompletableFuture<Void> oldLedgerWrite = markDelete(cursor, second);
        Awaitility.await().until(() -> second.equals(cursor.inProgressMarkDeletePersistPosition));
        cursor.startCreatingNewMetadataLedger();
        CompletableFuture<Void> queuedMarkDelete = markDelete(cursor, third);
        cursor.getAndSetState(ManagedCursorImpl.State.Deleting);
        oldLedgerWriteGate.complete(null);

        oldLedgerWrite.get(10, TimeUnit.SECONDS);
        assertThat(queuedMarkDelete.handle((ignored, error) -> error).get(10, TimeUnit.SECONDS)).isNotNull();
        assertThat(bkc.getLedgers()).containsExactlyInAnyOrderElementsOf(ledgersBeforeSwitch);
    }

    private CompletableFuture<Void> markDelete(ManagedCursorImpl cursor, Position position) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        cursor.asyncMarkDelete(position, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future;
    }

    private CompletableFuture<Void> close(ManagedCursorImpl cursor) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future;
    }

    private CompletableFuture<Void> resetCursor(ManagedCursorImpl cursor, Position position) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        cursor.asyncResetCursor(position, false, new AsyncCallbacks.ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    private ManagedCursorInfo readCursorInfo(ManagedLedgerImpl ledger, ManagedCursorImpl cursor) throws Exception {
        CompletableFuture<ManagedCursorInfo> future = new CompletableFuture<>();
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), cursor.getName(),
                new MetaStoreCallback<ManagedCursorInfo>() {
                    @Override
                    public void operationComplete(ManagedCursorInfo result, Stat stat) {
                        future.complete(result);
                    }

                    @Override
                    public void operationFailed(MetaStoreException exception) {
                        future.completeExceptionally(exception);
                    }
                });
        return future.get(10, TimeUnit.SECONDS);
    }

    private void assertCursorInfoPosition(ManagedCursorInfo info, Position position) {
        assertThat(info.getMarkDeleteLedgerId()).isEqualTo(position.getLedgerId());
        assertThat(info.getMarkDeleteEntryId()).isEqualTo(position.getEntryId());
        assertThat(info.getCursorsLedgerId()).isEqualTo(-1);
    }

    private boolean completesWithinOneSecond(CompletableFuture<Void> future) throws Exception {
        try {
            future.get(1, TimeUnit.SECONDS);
            return true;
        } catch (TimeoutException expected) {
            return false;
        }
    }
}

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.MarkDeleteEntry;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.testng.annotations.Test;

public class ManagedCursorResetFailedRebuildTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 30000)
    public void testRebuildingCursorLedgerAfterFailedResetPreservesAcknowledgments() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0)
                .setPersistIndividualAckAsLongArray(false);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-failed-cursor-ledger-rebuild", config);
        ManagedCursorImpl originalCursor = (ManagedCursorImpl) ledger.openCursor("sub");
        ManagedCursorImpl cursor = spy(originalCursor);
        ledger.getCursors().removeCursor(cursor.getName());
        ledger.getCursors().add(cursor, null);
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        cursor.markDelete(positions.get(0));
        Position previouslyAcknowledged = positions.get(8);
        cursor.delete(previouslyAcknowledged);
        assertThat(lastPersistedCursorPosition(cursor.cursorLedger).getIndividualDeletedMessagesCount()).isEqualTo(1);

        bkc.addEntryFailAfter(0, BKException.Code.NoBookieAvailableException);
        metadataStore.failConditional(new MetadataStoreException("reset metadata-store write failed"),
                (operation, path) -> operation == OperationType.PUT
                        && path.equals("/managed-ledgers/" + ledger.getName() + "/sub"));
        // The reset has the same mark-delete position, isolating the ACK state from position rollback behavior.
        assertThatThrownBy(() -> cursor.resetCursor(positions.get(1)))
                .isInstanceOf(InvalidCursorPositionException.class);
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(positions.get(0));
        assertThat(cursor.isMessageDeleted(previouslyAcknowledged)).isTrue();
        assertThat(cursor.getState()).isEqualTo("NoLedger");

        LedgerHandle failedCursorLedger = cursor.cursorLedger;
        AtomicBoolean captureOrdinaryWrite = new AtomicBoolean(true);
        CompletableFuture<DeferredPersist> deferredWrite = new CompletableFuture<>();
        doAnswer(invocation -> {
            LedgerHandle targetLedger = invocation.getArgument(0);
            if (targetLedger != failedCursorLedger && targetLedger == cursor.cursorLedger
                    && captureOrdinaryWrite.compareAndSet(true, false)) {
                // The initial write to the new ledger has completed and its metadata pointer has switched.
                // Defer the queued ordinary flush without blocking the ledger executor.
                deferredWrite.complete(new DeferredPersist(targetLedger, invocation.getArgument(1),
                        invocation.getArgument(2), invocation.getArgument(3)));
                return null;
            }
            return invocation.callRealMethod();
        }).when(cursor).persistPositionToLedger(any(LedgerHandle.class), any(MarkDeleteEntry.class),
                any(VoidCallback.class), anyBoolean());

        try {
            cursor.flush();
            DeferredPersist ordinaryWrite = deferredWrite.get(5, TimeUnit.SECONDS);
            ManagedCursorInfo cursorInfo = storedCursorInfo(ledger);
            assertThat(cursorInfo.getCursorsLedgerId()).isEqualTo(ordinaryWrite.ledgerHandle().getId());
            assertThat(ordinaryWrite.ledgerHandle().getLastAddConfirmed()).isZero();
            PositionInfo initialSnapshot = lastPersistedCursorPosition(ordinaryWrite.ledgerHandle());
            assertThat(initialSnapshot.getLedgerId()).isEqualTo(positions.get(0).getLedgerId());
            assertThat(initialSnapshot.getEntryId()).isEqualTo(positions.get(0).getEntryId());
            assertThat(initialSnapshot.getIndividualDeletedMessagesCount())
                    .as("the durable initial snapshot must preserve ACKs from before the failed reset")
                    .isEqualTo(1);
            MessageRange range = initialSnapshot.getIndividualDeletedMessageAt(0);
            assertThat(range.getLowerEndpoint().getLedgerId()).isEqualTo(previouslyAcknowledged.getLedgerId());
            assertThat(range.getLowerEndpoint().getEntryId()).isEqualTo(previouslyAcknowledged.getEntryId() - 1);
            assertThat(range.getUpperEndpoint().getLedgerId()).isEqualTo(previouslyAcknowledged.getLedgerId());
            assertThat(range.getUpperEndpoint().getEntryId()).isEqualTo(previouslyAcknowledged.getEntryId());
        } finally {
            captureOrdinaryWrite.set(false);
            DeferredPersist ordinaryWrite = deferredWrite.getNow(null);
            if (ordinaryWrite != null) {
                resumePersist(cursor, ordinaryWrite);
            }
        }
    }

    private record DeferredPersist(LedgerHandle ledgerHandle, MarkDeleteEntry entry, VoidCallback callback,
                                   boolean ignoreClosedStateAfterFailure) {
    }

    private static void resumePersist(ManagedCursorImpl cursor, DeferredPersist deferred) throws Exception {
        CompletableFuture<Void> completed = new CompletableFuture<>();
        cursor.ledger.getExecutor().execute(() -> cursor.persistPositionToLedger(deferred.ledgerHandle(),
                deferred.entry(), new VoidCallback() {
                    @Override
                    public void operationComplete() {
                        deferred.callback().operationComplete();
                        completed.complete(null);
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException exception) {
                        deferred.callback().operationFailed(exception);
                        completed.completeExceptionally(exception);
                    }
                }, deferred.ignoreClosedStateAfterFailure()));
        completed.get(5, TimeUnit.SECONDS);
    }

    private static ManagedCursorInfo storedCursorInfo(ManagedLedgerImpl ledger) throws Exception {
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

    private static PositionInfo lastPersistedCursorPosition(LedgerHandle ledgerHandle) throws Exception {
        CompletableFuture<PositionInfo> result = new CompletableFuture<>();
        long entryId = ledgerHandle.getLastAddConfirmed();
        ledgerHandle.asyncReadEntries(entryId, entryId, (rc, lh, entries, ctx) -> {
            try {
                if (rc != BKException.Code.OK) {
                    throw BKException.create(rc);
                }
                PositionInfo positionInfo = new PositionInfo();
                positionInfo.parseFrom(entries.nextElement().getEntry());
                result.complete(positionInfo);
            } catch (Exception exception) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result.get(5, TimeUnit.SECONDS);
    }
}

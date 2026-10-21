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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.InvalidCursorPositionException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Verifies that a completed reset persists the acknowledgment state it exposes in memory. */
@Test(timeOut = 30000)
public class ManagedCursorResetDurabilityTest extends MockedBookKeeperTestCase {

    @DataProvider(name = "successfulResetCases")
    public Object[][] successfulResetCases() {
        return new Object[][] {{false, false}, {true, true}};
    }

    @DataProvider(name = "batchIndexPersistenceSettings")
    public Object[][] batchIndexPersistenceSettings() {
        return new Object[][] {{true, 0}, {false, 10000}};
    }

    @Test(dataProvider = "successfulResetCases")
    public void testResetPersistsAcknowledgmentState(boolean persistAsLongArray,
            boolean requestedBatchAcknowledgment) throws Exception {
        ManagedLedgerConfig config = defaultConfig()
                .setThrottleMarkDelete(0)
                .setPersistIndividualAckAsLongArray(persistAsLongArray);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "reset-success-" + persistAsLongArray + "-" + requestedBatchAcknowledgment, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = addEntries(ledger, 6);
        Position resetPosition = positions.get(2);
        Position individuallyAcknowledged = positions.get(3);
        Position laterBatch = positions.get(4);
        cursor.markDelete(positions.get(0));
        cursor.delete(individuallyAcknowledged);
        cursor.delete(withAckSet(resetPosition, 4L));
        cursor.delete(withAckSet(laterBatch, 2L));

        cursor.resetCursor(requestedBatchAcknowledgment ? withAckSet(resetPosition, 6L) : resetPosition);

        PositionInfo persisted = lastPersistedCursorPosition(cursor);
        assertThat(persisted.getLedgerId()).isEqualTo(positions.get(1).getLedgerId());
        assertThat(persisted.getEntryId()).isEqualTo(positions.get(1).getEntryId());
        assertThat(persisted.getIndividualDeletedMessagesCount()).isZero();
        assertThat(persisted.getIndividualDeletedMessageRangesCount()).isZero();
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount())
                .isEqualTo(requestedBatchAcknowledgment ? 1 : 0);
        if (requestedBatchAcknowledgment) {
            BatchedEntryDeletionIndexInfo batchInfo = persisted.getBatchedEntryDeletionIndexInfoAt(0);
            assertThat(batchInfo.getPosition().getLedgerId()).isEqualTo(resetPosition.getLedgerId());
            assertThat(batchInfo.getPosition().getEntryId()).isEqualTo(resetPosition.getEntryId());
            assertThat(batchInfo.getDeleteSetsCount()).isEqualTo(1);
            assertThat(batchInfo.getDeleteSetAt(0)).isEqualTo(6L);
        }

        recoverPersistedCursor(cursor);

        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(positions.get(1));
        assertThat(cursor.getReadPosition()).isEqualTo(resetPosition);
        assertThat(cursor.isMessageDeleted(individuallyAcknowledged)).isFalse();
        assertThat(cursor.getBatchPositionAckSet(laterBatch)).isNull();
        if (requestedBatchAcknowledgment) {
            assertThat(cursor.getBatchPositionAckSet(resetPosition)).containsExactly(6L);
        } else {
            assertThat(cursor.getBatchPositionAckSet(resetPosition)).isNull();
        }
    }

    @Test
    public void testResetPublishesCheckpointWhenOverrideRunsAlignmentDirectly() throws Exception {
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "reset-direct-alignment-override", defaultConfig().setThrottleMarkDelete(0));
        List<Position> positions = addEntries(ledger, 4);
        NonDurableCursorImpl cursor = new NonDurableCursorImpl(
                bkc, ledger, "non-durable", null, CommandSubscribe.InitialPosition.Latest, false) {
            @Override
            protected void internalAsyncMarkDelete(Position newPosition, Map<String, Long> properties,
                    MarkDeleteCallback callback, Object ctx, Runnable alignAcknowledgeStatusAfterPersisted) {
                alignAcknowledgeStatusAfterPersisted.run();
                callback.markDeleteComplete(ctx);
            }
        };
        assertThat(cursor.lastMarkDeleteEntry.newPosition).isEqualTo(positions.get(3));

        cursor.resetCursor(positions.get(2));

        Position expectedMarkDelete = positions.get(1);
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(expectedMarkDelete);
        assertThat(cursor.lastMarkDeleteEntry.newPosition).isEqualTo(expectedMarkDelete);
    }

    @Test
    public void testResetPersistsAcknowledgmentsThroughMetadataStoreFallback() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-metadata-store-fallback", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = addEntries(ledger, 6);
        Position resetPosition = positions.get(2);
        Position laterBatch = positions.get(4);
        cursor.markDelete(positions.get(0));
        cursor.delete(positions.get(3));
        cursor.delete(withAckSet(resetPosition, 4L));
        cursor.delete(withAckSet(laterBatch, 2L));

        bkc.addEntryFailAfter(0, BKException.Code.NoBookieAvailableException);
        long[] resetAckSet = {6L};
        cursor.resetCursor(withAckSet(resetPosition, resetAckSet));

        ManagedCursorInfo persisted = storedCursorInfo(ledger);
        assertThat(persisted.getCursorsLedgerId()).as("the reset used the metadata-store fallback").isEqualTo(-1L);
        assertThat(persisted.getIndividualDeletedMessagesCount()).isZero();
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(1);
        BatchedEntryDeletionIndexInfo batchInfo = persisted.getBatchedEntryDeletionIndexInfoAt(0);
        assertThat(batchInfo.getPosition().getLedgerId()).isEqualTo(resetPosition.getLedgerId());
        assertThat(batchInfo.getPosition().getEntryId()).isEqualTo(resetPosition.getEntryId());
        assertThat(batchInfo.getDeleteSetsCount()).isEqualTo(1);
        assertThat(batchInfo.getDeleteSetAt(0)).isEqualTo(resetAckSet[0]);

        CompletableFuture<Void> recovered = new CompletableFuture<>();
        // Recover from the stored snapshot before close can replace it with the in-memory state.
        cursor.recover(recoveryCallback(recovered));
        recovered.get(5, TimeUnit.SECONDS);

        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(positions.get(1));
        assertThat(cursor.getReadPosition()).isEqualTo(resetPosition);
        assertThat(cursor.isMessageDeleted(positions.get(3))).isFalse();
        assertThat(cursor.getBatchPositionAckSet(laterBatch)).isNull();
        assertThat(cursor.getBatchPositionAckSet(resetPosition)).containsExactly(resetAckSet);
    }

    @Test
    public void testFailedResetPreservesAcknowledgmentsAndPositions() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-persistence-failure", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = addEntries(ledger, 6);
        Position resetPosition = positions.get(2);
        Position laterBatch = positions.get(4);
        cursor.markDelete(positions.get(0));
        cursor.delete(positions.get(3));
        cursor.delete(withAckSet(resetPosition, 4L));
        cursor.delete(withAckSet(laterBatch, 2L));
        List<Entry> entries = cursor.readEntries(6);
        entries.forEach(Entry::release);
        Position previousMarkDelete = cursor.getMarkDeletedPosition();
        Position previousRead = cursor.getReadPosition();

        bkc.addEntryFailAfter(0, BKException.Code.NoBookieAvailableException);
        metadataStore.failConditional(new MetadataStoreException("reset metadata-store write failed"),
                (operation, path) -> operation == OperationType.PUT
                        && path.equals("/managed-ledgers/" + ledger.getName() + "/sub"));

        assertThatThrownBy(() -> cursor.resetCursor(withAckSet(resetPosition, 6L)))
                .isInstanceOf(InvalidCursorPositionException.class);

        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(previousMarkDelete);
        assertThat(cursor.getReadPosition()).isEqualTo(previousRead);
        assertThat(cursor.isMessageDeleted(positions.get(3))).isTrue();
        assertThat(cursor.getBatchPositionAckSet(resetPosition)).containsExactly(4L);
        assertThat(cursor.getBatchPositionAckSet(laterBatch)).containsExactly(2L);
    }

    @Test(dataProvider = "batchIndexPersistenceSettings")
    public void testResetHonorsBatchIndexPersistenceSettings(boolean batchIndexAckEnabled, int maxIndexes)
            throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0);
        config.setDeletionAtBatchIndexLevelEnabled(batchIndexAckEnabled);
        config.setMaxBatchDeletedIndexToPersist(maxIndexes);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "my-tenant/my-ns/persistent/reset-batch-settings-" + batchIndexAckEnabled, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = addEntries(ledger, 4);
        Position resetPosition = positions.get(1);

        cursor.resetCursor(withAckSet(resetPosition, 6L));

        if (batchIndexAckEnabled) {
            assertThat(cursor.getBatchPositionAckSet(resetPosition)).containsExactly(6L);
        } else {
            assertThat(cursor.getBatchPositionAckSet(resetPosition)).isNull();
        }
        PositionInfo persisted = lastPersistedCursorPosition(cursor);
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount())
                .as("a reset must respect the configured batch ACK persistence limit and feature switch")
                .isZero();
    }

    private static Position withAckSet(Position position, long... ackSet) {
        return AckSetStateUtil.createPositionWithAckSet(position.getLedgerId(), position.getEntryId(), ackSet);
    }

    private static List<Position> addEntries(ManagedLedgerImpl ledger, int count) throws Exception {
        List<Position> positions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        return positions;
    }

    private static void recoverPersistedCursor(ManagedCursorImpl cursor) throws Exception {
        Position markDelete = cursor.getMarkDeletedPosition();
        ManagedCursorInfo info = new ManagedCursorInfo()
                .setCursorsLedgerId(cursor.getCursorLedger())
                .setMarkDeleteLedgerId(markDelete.getLedgerId())
                .setMarkDeleteEntryId(markDelete.getEntryId());
        CompletableFuture<Void> recovered = new CompletableFuture<>();
        // Read the durable cursor ledger before close can rewrite the in-memory reset state.
        cursor.recoverFromLedger(info, recoveryCallback(recovered));
        recovered.get(5, TimeUnit.SECONDS);
    }

    private static VoidCallback recoveryCallback(CompletableFuture<Void> recovered) {
        return new VoidCallback() {
            @Override
            public void operationComplete() {
                recovered.complete(null);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                recovered.completeExceptionally(exception);
            }
        };
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

    private static PositionInfo lastPersistedCursorPosition(ManagedCursorImpl cursor) throws Exception {
        CompletableFuture<PositionInfo> result = new CompletableFuture<>();
        long entryId = cursor.cursorLedger.getLastAddConfirmed();
        cursor.cursorLedger.asyncReadEntries(entryId, entryId, (rc, lh, entries, ctx) -> {
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

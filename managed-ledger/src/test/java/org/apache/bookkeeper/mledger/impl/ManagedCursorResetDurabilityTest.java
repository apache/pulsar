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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
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
import org.apache.bookkeeper.mledger.util.MockClock;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore.OperationType;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Verifies that a completed reset persists the acknowledgment state it exposes in memory. */
@Test(timeOut = 30000)
public class ManagedCursorResetDurabilityTest extends MockedBookKeeperTestCase {

    @DataProvider(name = "individualAckFormats")
    public Object[][] individualAckFormats() {
        return new Object[][] {{false}, {true}};
    }

    @DataProvider(name = "batchIndexPersistenceSettings")
    public Object[][] batchIndexPersistenceSettings() {
        return new Object[][] {{true, 0}, {false, 10000}};
    }

    @Test(dataProvider = "individualAckFormats")
    public void testResetClearsPersistedIndividualAcknowledgments(boolean persistAsLongArray) throws Exception {
        ManagedLedgerConfig config = defaultConfig()
                .setThrottleMarkDelete(0)
                .setPersistIndividualAckAsLongArray(persistAsLongArray);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-individual-acks-" + persistAsLongArray,
                config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = addEntries(ledger, 5);

        cursor.delete(positions.get(3));
        cursor.resetCursor(positions.get(1));
        assertThat(cursor.isMessageDeleted(positions.get(3)))
                .as("reset makes the previously acknowledged entry available for replay")
                .isFalse();

        recoverPersistedCursor(cursor);

        List<Entry> replayed = cursor.readEntries(5);
        try {
            assertThat(replayed).extracting(Entry::getPosition)
                    .as("recovery must preserve every entry made available by the completed reset")
                    .containsExactlyElementsOf(positions.subList(1, 5));
        } finally {
            replayed.forEach(Entry::release);
        }
    }

    @Test
    public void testResetClearsPersistedBatchAcknowledgments() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-batch-acks", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = addEntries(ledger, 4);
        Position batch = positions.get(3);
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                batch.getLedgerId(), batch.getEntryId(), new long[] {2L}));

        cursor.resetCursor(positions.get(1));
        assertThat(cursor.getBatchPositionAckSet(batch)).isNull();

        recoverPersistedCursor(cursor);

        assertThat(cursor.getBatchPositionAckSet(batch))
                .as("batch acknowledgments cleared by reset must remain cleared after recovery")
                .isNull();
    }

    @Test
    public void testResetPersistsRequestedBatchAcknowledgments() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-requested-batch-acks", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = addEntries(ledger, 3);
        Position batch = positions.get(1);
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                batch.getLedgerId(), batch.getEntryId(), new long[] {4L}));
        long[] resetAckSet = {6L};

        cursor.resetCursor(AckSetStateUtil.createPositionWithAckSet(
                batch.getLedgerId(), batch.getEntryId(), resetAckSet));
        assertThat(cursor.getBatchPositionAckSet(batch)).containsExactly(resetAckSet);

        recoverPersistedCursor(cursor);

        assertThat(cursor.getBatchPositionAckSet(batch))
                .as("the batch position requested by reset must survive recovery")
                .containsExactly(resetAckSet);
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
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                resetPosition.getLedgerId(), resetPosition.getEntryId(), new long[] {4L}));
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                laterBatch.getLedgerId(), laterBatch.getEntryId(), new long[] {2L}));

        long previousMetadataStoreWrites = cursor.getStats().getPersistZookeeperSucceed();
        bkc.addEntryFailAfter(0, BKException.Code.NoBookieAvailableException);
        long[] resetAckSet = {6L};
        cursor.resetCursor(AckSetStateUtil.createPositionWithAckSet(
                resetPosition.getLedgerId(), resetPosition.getEntryId(), resetAckSet));

        assertThat(cursor.getStats().getPersistZookeeperSucceed()).isGreaterThan(previousMetadataStoreWrites);
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
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                resetPosition.getLedgerId(), resetPosition.getEntryId(), new long[] {4L}));
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                laterBatch.getLedgerId(), laterBatch.getEntryId(), new long[] {2L}));
        List<Entry> entries = cursor.readEntries(6);
        entries.forEach(Entry::release);
        Position previousMarkDelete = cursor.getMarkDeletedPosition();
        Position previousRead = cursor.getReadPosition();
        long previousConsumed = cursor.getMessagesConsumedCounter();
        long previousLedgerErrors = cursor.getStats().getPersistLedgerErrors();
        long previousMetadataStoreErrors = cursor.getStats().getPersistZookeeperErrors();

        bkc.addEntryFailAfter(0, BKException.Code.NoBookieAvailableException);
        metadataStore.failConditional(new MetadataStoreException("reset metadata-store write failed"),
                (operation, path) -> operation == OperationType.PUT
                        && path.equals("/managed-ledgers/" + ledger.getName() + "/sub"));

        assertThatThrownBy(() -> cursor.resetCursor(AckSetStateUtil.createPositionWithAckSet(
                resetPosition.getLedgerId(), resetPosition.getEntryId(), new long[] {6L})))
                .isInstanceOf(InvalidCursorPositionException.class);

        assertThat(cursor.getStats().getPersistLedgerErrors()).isGreaterThan(previousLedgerErrors);
        assertThat(cursor.getStats().getPersistZookeeperErrors()).isGreaterThan(previousMetadataStoreErrors);
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(previousMarkDelete);
        assertThat(cursor.getReadPosition()).isEqualTo(previousRead);
        assertThat(cursor.getMessagesConsumedCounter()).isEqualTo(previousConsumed);
        assertThat(cursor.isMessageDeleted(positions.get(2))).isFalse();
        assertThat(cursor.isMessageDeleted(positions.get(3))).isTrue();
        assertThat(cursor.getBatchPositionAckSet(resetPosition)).containsExactly(4L);
        assertThat(cursor.getBatchPositionAckSet(laterBatch)).containsExactly(2L);
    }

    @Test
    public void testAcknowledgmentsAfterResetAndCursorLedgerRolloverArePersisted() throws Exception {
        MockClock clock = new MockClock();
        ManagedLedgerConfig config = defaultConfig().setThrottleMarkDelete(0)
                .setClock(clock).setLedgerRolloverTimeout(5);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("reset-cursor-ledger-rollover", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = addEntries(ledger, 6);
        Position resetPosition = positions.get(1);
        cursor.resetCursor(AckSetStateUtil.createPositionWithAckSet(
                resetPosition.getLedgerId(), resetPosition.getEntryId(), new long[] {6L}));
        long previousCursorLedger = cursor.getCursorLedger();

        clock.advance(6, TimeUnit.SECONDS);
        assertThat(cursor.periodicRollover()).isTrue();
        Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(cursor.getCursorLedger()).isNotEqualTo(previousCursorLedger);
            assertThat(cursor.getState()).isEqualTo("Open");
        });

        // Ordinary ACKs after rollover must persist the live state rather than the earlier reset snapshot.
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                resetPosition.getLedgerId(), resetPosition.getEntryId(), new long[] {4L}));
        cursor.delete(positions.get(3));
        recoverPersistedCursor(cursor);

        assertThat(cursor.getBatchPositionAckSet(resetPosition)).containsExactly(4L);
        assertThat(cursor.isMessageDeleted(positions.get(2))).isFalse();
        assertThat(cursor.isMessageDeleted(positions.get(3))).isTrue();
        assertThat(cursor.getReadPosition()).isEqualTo(resetPosition);
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
        cursor.delete(positions.get(3));

        cursor.resetCursor(AckSetStateUtil.createPositionWithAckSet(
                resetPosition.getLedgerId(), resetPosition.getEntryId(), new long[] {6L}));

        if (batchIndexAckEnabled) {
            assertThat(cursor.getBatchPositionAckSet(resetPosition)).containsExactly(6L);
        } else {
            assertThat(cursor.getBatchPositionAckSet(resetPosition)).isNull();
        }
        PositionInfo persisted = lastPersistedCursorPosition(cursor);
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount())
                .as("a reset must respect the configured batch ACK persistence limit and feature switch")
                .isZero();
        assertThat(persisted.getIndividualDeletedMessagesCount()).isZero();
        assertThat(persisted.getIndividualDeletedMessageRangesCount()).isZero();
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

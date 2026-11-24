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
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Verifies recovery of partial batch acknowledgments from cursor metadata. */
@Test(timeOut = 30000)
public class ManagedCursorBatchAckRecoveryTest extends MockedBookKeeperTestCase {

    @DataProvider(name = "batchRecovery")
    public Object[][] batchRecovery() {
        return new Object[][] {{1, false}, {2, false}, {3, false}, {1, true}, {2, true}, {3, true}};
    }

    @DataProvider(name = "booleans")
    public Object[][] booleans() {
        return new Object[][] {{false}, {true}};
    }

    @Test
    public void testIsCursorDataFullyPersistableReflectsBatchDeletedIndexLimit() throws Exception {
        ManagedLedgerConfig config = defaultConfig();
        config.setDeletionAtBatchIndexLevelEnabled(true);
        config.setMaxBatchDeletedIndexToPersist(2);
        config.setThrottleMarkDelete(0);
        String name = "tenant/ns/persistent/cursor-data-fully-persistable-batch-index";
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i <= 3; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        cursor.delete(positions.get(0));
        Awaitility.await().until(() -> cursor.getStats().getPersistLedgerSucceed() > 0);

        long[][] ackSets = {{2L}, {Long.MIN_VALUE, 1L}, {5L, 0L, 3L}};
        for (int i = 1; i < positions.size(); i++) {
            Position position = positions.get(i);
            cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                    position.getLedgerId(), position.getEntryId(), ackSets[i - 1]));
            if (i < 3) {
                assertThat(cursor.isCursorDataFullyPersistable())
                        .as("batch deleted index count %s is within limit 2", i)
                        .isTrue();
            } else {
                assertThat(cursor.isCursorDataFullyPersistable())
                        .as("third batch deleted index record exceeds limit 2")
                        .isFalse();
            }
        }
    }

    @Test(dataProvider = "booleans")
    public void testCumulativeAckPreservesIndividualBatchAcks(boolean metadataStore) throws Exception {
        ManagedLedgerConfig config = defaultConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(metadataStore ? 20 : -1);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        String name = "tenant/ns/persistent/mixed-batch-acks-" + metadataStore;
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position initial = ledger.addEntry(new byte[] {0});
        Position batch = ledger.addEntry(new byte[] {1});
        ledger.addEntry(new byte[] {2});
        cursor.markDelete(initial);

        // The individual ACK clears index 3; the cumulative ACK clears indexes 0 and 1.
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                batch.getLedgerId(), batch.getEntryId(), new long[] {7L}));
        cursor.markDelete(AckSetStateUtil.createPositionWithAckSet(
                batch.getLedgerId(), batch.getEntryId(), new long[] {12L}));
        assertThat(cursor.getBatchPositionAckSet(batch)).containsExactly(4L);

        // An older cumulative snapshot must not bring any acknowledged indexes back either.
        cursor.markDelete(AckSetStateUtil.createPositionWithAckSet(
                batch.getLedgerId(), batch.getEntryId(), new long[] {14L}));
        assertThat(cursor.getBatchPositionAckSet(batch)).containsExactly(4L);
        ledger.close();

        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered.getMarkDeletedPosition()).isEqualTo(initial);
        assertThat(recovered.getBatchPositionAckSet(batch)).containsExactly(4L);
    }

    @Test(dataProvider = "booleans")
    public void testCumulativeAckOverlapsIndividualBatchAck(boolean fullyAcknowledged) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setMaxUnackedRangesToPersistInMetadataStore(-1);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "tenant/ns/persistent/concurrent-batch-acks-" + fullyAcknowledged, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position initial = ledger.addEntry(new byte[] {0});
        Position batch = ledger.addEntry(new byte[] {1});
        ledger.addEntry(new byte[] {2});
        cursor.markDelete(initial);
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                batch.getLedgerId(), batch.getEntryId(), new long[] {14L}));

        CountDownLatch individualMerged = new CountDownLatch(1);
        CountDownLatch releaseIndividual = new CountDownLatch(1);
        Position individualPosition = AckSetStateUtil.createPositionWithAckSet(
                batch.getLedgerId(), batch.getEntryId(), new long[] {fullyAcknowledged ? 3L : 11L});
        Iterable<Position> individualPositions = () -> new Iterator<>() {
            private final Iterator<Position> delegate = List.of(individualPosition).iterator();

            @Override
            public boolean hasNext() {
                boolean hasNext = delegate.hasNext();
                if (!hasNext) {
                    // Pause after the real individual merge, while asyncDelete still holds the write lock.
                    individualMerged.countDown();
                    awaitLatch(releaseIndividual);
                }
                return hasNext;
            }

            @Override
            public Position next() {
                return delegate.next();
            }
        };
        CountDownLatch cumulativeEntered = new CountDownLatch(1);
        Position cumulativePosition = new AckSetPositionImpl(batch.getLedgerId(), batch.getEntryId(),
                new long[] {12L}) {
            @Override
            public long[] getAckSet() {
                cumulativeEntered.countDown();
                return super.getAckSet();
            }
        };
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("individual-batch-ack"));
        Future<?> individual = executor.submit(() -> {
            cursor.delete(individualPositions);
            return null;
        });
        CompletableFuture<Void> cumulative = new CompletableFuture<>();
        Thread cumulativeThread = new DefaultThreadFactory("cumulative-batch-ack").newThread(() -> {
            try {
                cursor.markDelete(cumulativePosition);
                cumulative.complete(null);
            } catch (Exception e) {
                cumulative.completeExceptionally(e);
            }
        });
        try {
            assertThat(individualMerged.await(5, TimeUnit.SECONDS)).isTrue();
            cumulativeThread.start();
            assertThat(cumulativeEntered.await(5, TimeUnit.SECONDS)).isTrue();
            // Both versions eventually wait for the cursor write lock. Before the fix, the cumulative
            // path has already replaced the bitmap by this point, restoring individually ACKed bits.
            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .until(() -> cumulativeThread.getState() == Thread.State.WAITING);
        } finally {
            releaseIndividual.countDown();
            cumulativeThread.join(5000);
        }
        individual.get(5, TimeUnit.SECONDS);
        cumulative.get(5, TimeUnit.SECONDS);

        if (fullyAcknowledged) {
            assertThat(cursor.getBatchPositionAckSet(batch)).isNull();
            assertThat(cursor.isMessageDeleted(batch)).isTrue();
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(batch);
        } else {
            assertThat(cursor.getBatchPositionAckSet(batch)).containsExactly(8L);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(initial);
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testFlushPersistsPartialBatchAckWithoutIndividualRanges() throws Exception {
        ManagedLedgerConfig config = defaultConfig().setMaxUnackedRangesToPersistInMetadataStore(-1);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        config.setMaxBatchDeletedIndexToPersist(2);
        String name = "tenant/ns/persistent/partial-batch-ack-flush";
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");

        Position initialMarkDelete = ledger.addEntry(new byte[] {0});
        Position partiallyAcked = ledger.addEntry(new byte[] {1});
        ledger.addEntry(new byte[] {2});

        cursor.delete(initialMarkDelete);
        long persistedEntry = cursor.cursorLedger.getLastAddConfirmed();
        assertThat(lastPersistedCursorPosition(cursor).getBatchedEntryDeletionIndexInfosCount()).isZero();

        // The bitmap records the first message as acknowledged while leaving the second pending.
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                partiallyAcked.getLedgerId(), partiallyAcked.getEntryId(), new long[] {2L}));
        assertThat(cursor.getIndividuallyDeletedMessagesSet().size()).isZero();

        cursor.flush();

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> cursor.cursorLedger.getLastAddConfirmed() > persistedEntry);
        PositionInfo persisted = lastPersistedCursorPosition(cursor);
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(1);
        BatchedEntryDeletionIndexInfo ackInfo = persisted.getBatchedEntryDeletionIndexInfoAt(0);
        assertThat(ackInfo.getPosition().getLedgerId()).isEqualTo(partiallyAcked.getLedgerId());
        assertThat(ackInfo.getPosition().getEntryId()).isEqualTo(partiallyAcked.getEntryId());
        assertThat(deleteSet(ackInfo)).containsExactly(2L);
    }

    @Test(dataProvider = "batchRecovery")
    public void testPartialBatchAckPersistenceRetainsConfiguredCount(int batchCount, boolean metadataStore)
            throws Exception {
        ManagedLedgerConfig config = defaultConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(metadataStore ? 20 : -1);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        config.setMaxBatchDeletedIndexToPersist(2);
        String name = "tenant/ns/persistent/partial-batch-recovery-" + batchCount + "-" + metadataStore;
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i <= batchCount; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        long[][] ackSets = {{2L}, {Long.MIN_VALUE, 1L}, {5L, 0L, 3L}};
        for (int i = 1; i < positions.size(); i++) {
            Position position = positions.get(i);
            cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                    position.getLedgerId(), position.getEntryId(), ackSets[i - 1]));
        }

        // Advance mark-delete, then close to persist and reload the actual durable state.
        cursor.delete(positions.get(0));
        ledger.close();
        ManagedCursorInfo persisted = storedCursorInfo(ledger);
        if (metadataStore) {
            assertThat(persisted.getCursorsLedgerId()).isEqualTo(-1L);
            int expectedPersistedCount = Math.min(batchCount, 2);
            assertThat(persisted.getBatchedEntryDeletionIndexInfosCount())
                    .as("the partial ACK records were actually written before recovery")
                    .isEqualTo(expectedPersistedCount);
            for (int i = 0; i < expectedPersistedCount; i++) {
                BatchedEntryDeletionIndexInfo indexInfo = persisted.getBatchedEntryDeletionIndexInfoAt(i);
                assertThat(indexInfo.getPosition().getLedgerId()).isEqualTo(positions.get(i + 1).getLedgerId());
                assertThat(indexInfo.getPosition().getEntryId()).isEqualTo(positions.get(i + 1).getEntryId());
                assertThat(deleteSet(indexInfo)).containsExactly(ackSets[i]);
            }
        } else {
            assertThat(persisted.getCursorsLedgerId()).isGreaterThanOrEqualTo(0);
        }

        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered).isNotSameAs(cursor);
        for (int i = 1; i <= batchCount; i++) {
            long[] recoveredAckSet = recovered.getBatchPositionAckSet(positions.get(i));
            if (i <= 2) {
                assertThat(recoveredAckSet).containsExactly(ackSets[i - 1]);
            } else {
                assertThat(recoveredAckSet).as("ACK state above the configured batch cap is truncated").isNull();
            }
        }
    }

    @Test
    public void testMetadataStoreRecoveryPreservesAllCursorState() throws Exception {
        ManagedLedgerConfig config = defaultConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        String name = "tenant/ns/persistent/combined-metadata-store-recovery";
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }

        cursor.markDelete(positions.get(0), Map.of("mark-delete-property", 7L));
        cursor.setCursorProperties(Map.of("cursor-property", "value")).get(5, TimeUnit.SECONDS);
        cursor.delete(positions.get(2));
        long[] ackSet = {Long.MIN_VALUE, 1L};
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                positions.get(4).getLedgerId(), positions.get(4).getEntryId(), ackSet));
        ledger.close();

        ManagedCursorInfo persisted = storedCursorInfo(ledger);
        assertThat(persisted.getCursorsLedgerId()).isEqualTo(-1L);
        assertThat(persisted.getIndividualDeletedMessagesCount()).isEqualTo(1);
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount()).isEqualTo(1);
        assertThat(deleteSet(persisted.getBatchedEntryDeletionIndexInfoAt(0))).containsExactly(ackSet);

        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, config);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered).isNotSameAs(cursor);
        assertThat(recovered.getMarkDeletedPosition()).isEqualTo(positions.get(0));
        assertThat(recovered.getProperties()).containsExactlyEntriesOf(Map.of("mark-delete-property", 7L));
        assertThat(recovered.getCursorProperties()).containsExactlyEntriesOf(Map.of("cursor-property", "value"));
        assertThat(recovered.isMessageDeleted(positions.get(1))).isFalse();
        assertThat(recovered.isMessageDeleted(positions.get(2))).isTrue();
        assertThat(recovered.getBatchPositionAckSet(positions.get(4))).containsExactly(ackSet);
    }

    @Test(dataProvider = "booleans")
    public void testMetadataStoreRecoveryHandlesMissingOrDisabledBatchIndexAck(boolean recoverBatchIndexAck)
            throws Exception {
        ManagedLedgerConfig writeConfig = defaultConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        writeConfig.setDeletionAtBatchIndexLevelEnabled(!recoverBatchIndexAck);
        String name = "tenant/ns/persistent/batch-metadata-compatibility-" + recoverBatchIndexAck;
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(name, writeConfig);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        Position position = ledger.addEntry(new byte[] {1});
        if (!recoverBatchIndexAck) {
            cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                    position.getLedgerId(), position.getEntryId(), new long[] {2L}));
        }
        ledger.close();

        ManagedCursorInfo persisted = storedCursorInfo(ledger);
        assertThat(persisted.getCursorsLedgerId()).isEqualTo(-1L);
        assertThat(persisted.getBatchedEntryDeletionIndexInfosCount())
                .isEqualTo(recoverBatchIndexAck ? 0 : 1);

        ManagedLedgerConfig recoveryConfig = defaultConfig()
                .setMaxUnackedRangesToPersistInMetadataStore(20);
        recoveryConfig.setDeletionAtBatchIndexLevelEnabled(recoverBatchIndexAck);
        @Cleanup
        ManagedLedgerImpl reopened = (ManagedLedgerImpl) factory.open(name, recoveryConfig);
        ManagedCursorImpl recovered = (ManagedCursorImpl) reopened.openCursor("sub");
        assertThat(recovered.getBatchPositionAckSet(position)).isNull();
    }

    private static long[] deleteSet(BatchedEntryDeletionIndexInfo indexInfo) {
        long[] result = new long[indexInfo.getDeleteSetsCount()];
        for (int i = 0; i < result.length; i++) {
            result[i] = indexInfo.getDeleteSetAt(i);
        }
        return result;
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
                if (rc != 0) {
                    throw new IllegalStateException("Failed to read cursor ledger entry: " + rc);
                }
                var entry = entries.nextElement();
                PositionInfo positionInfo = new PositionInfo();
                positionInfo.parseFrom(entry.getEntry());
                result.complete(positionInfo);
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
        }, null);
        return result.get(5, TimeUnit.SECONDS);
    }
}

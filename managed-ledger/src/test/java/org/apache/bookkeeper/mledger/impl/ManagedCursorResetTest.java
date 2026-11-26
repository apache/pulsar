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
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Regression coverage for cursor reset positions, acknowledgment state, and backlog counters. */
@Test(timeOut = 30000)
public class ManagedCursorResetTest extends MockedBookKeeperTestCase {

    private static final int ENTRY_COUNT = 16;

    @DataProvider(name = "resetPositions")
    public Object[][] resetPositions() {
        return new Object[][] {
                {"backward", 32, 5, 4},
                {"same-mark-delete", 32, 5, 6},
                {"clip-range", 32, 5, 9},
                {"ledger-boundary", 4, -1, 4},
                {"multiple-ledgers", 4, 5, 9},
                {"earliest", 4, 11, -1},
                {"latest", 32, 5, ENTRY_COUNT}
        };
    }

    @DataProvider(name = "ackCounts")
    public Object[][] ackCounts() {
        return new Object[][] {{0}, {1}};
    }

    @Test(dataProvider = "resetPositions")
    public void testResetAcrossAcknowledgmentRanges(String scenario, int entriesPerLedger, int markDeleteIndex,
                                                    int resetIndex) throws Exception {
        ManagedLedgerConfig config = config(entriesPerLedger)
                .setMaxUnackedRangesToPersist(1)
                .setMaxUnackedRangesToPersistInMetadataStore(1);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("tenant/ns/persistent/reset-" + scenario, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = appendEntries(ledger, ENTRY_COUNT);
        if (markDeleteIndex >= 0) {
            cursor.markDelete(positions.get(markDeleteIndex));
        }

        int ackCount = 0;
        for (int index : new int[] {2, 3, 4, 6, 8, 9, 10, 12, 13, 14}) {
            // Leave the first entry after the mark-delete position unacknowledged, so the
            // individual ACK ranges cannot advance the initial mark-delete position.
            if (index > markDeleteIndex + 1) {
                cursor.delete(positions.get(index));
                ackCount++;
            }
        }
        assertBacklog(cursor, ENTRY_COUNT - markDeleteIndex - 1L - ackCount);
        if (entriesPerLedger == 4 && markDeleteIndex < 5) {
            assertThat(cursor.getIndividuallyDeletedMessagesSet().asRanges())
                    .as("a later ledger's entry zero has an open lower bound of -1")
                    .anyMatch(range -> range.lowerEndpoint().getEntryId() == -1);
        }

        Position target = resetIndex < 0 ? PositionFactory.EARLIEST
                : resetIndex == ENTRY_COUNT ? PositionFactory.LATEST : positions.get(resetIndex);
        cursor.resetCursor(target);
        assertReset(cursor, positions, resetIndex);
    }

    @Test(dataProvider = "ackCounts")
    public void testResetWithEmptyOrSingletonAcknowledgments(int ackCount) throws Exception {
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(
                "tenant/ns/persistent/reset-empty-or-singleton", config(32));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = appendEntries(ledger, ENTRY_COUNT);
        cursor.markDelete(positions.get(3));
        if (ackCount > 0) {
            cursor.delete(positions.get(12));
            cursor.delete(positions.get(12));
        }
        assertBacklog(cursor, 12L - ackCount);

        cursor.resetCursor(positions.get(4));
        assertReset(cursor, positions, 4);
        cursor.resetCursor(positions.get(4));
        assertReset(cursor, positions, 4);
    }

    @Test
    public void testLongRangeBeyondPersistenceLimit() throws Exception {
        int count = 4096;
        ManagedLedgerConfig config = config(count + 1)
                .setMaxUnackedRangesToPersist(1)
                .setMaxUnackedRangesToPersistInMetadataStore(1);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("tenant/ns/persistent/reset-long-range", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = appendEntries(ledger, count);
        cursor.delete(positions.subList(1, count - 2));
        cursor.delete(positions.get(count - 1));
        assertThat(cursor.getIndividuallyDeletedMessagesSet().size()).isEqualTo(2);
        assertBacklog(cursor, 2);

        // Removing the prefix splits the long range, and the remaining suffix must be counted
        // in full even though only one range fits in a persisted snapshot.
        cursor.resetCursor(positions.get(count / 2));
        assertReset(cursor, positions, count / 2);
    }

    @Test
    public void testResetClearsPartialBatchAcknowledgments() throws Exception {
        ManagedLedgerConfig config = config(4);
        config.setDeletionAtBatchIndexLevelEnabled(true);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("tenant/ns/persistent/reset-batch-acks", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = appendEntries(ledger, ENTRY_COUNT);
        cursor.markDelete(positions.get(1));
        Position partial = positions.get(5);
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                partial.getLedgerId(), partial.getEntryId(), new long[] {5L}));
        Position whole = positions.get(7);
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                whole.getLedgerId(), whole.getEntryId(), new long[] {0L}));
        cursor.delete(positions.get(9));
        assertThat(cursor.getBatchPositionAckSet(partial)).containsExactly(5L);
        assertBacklog(cursor, 12);

        Position target = positions.get(4);
        cursor.resetCursor(AckSetStateUtil.createPositionWithAckSet(
                target.getLedgerId(), target.getEntryId(), new long[] {6L}));
        assertReset(cursor, positions, 4);
        assertThat(cursor.getBatchPositionAckSet(partial)).isNull();
        assertThat(cursor.getBatchPositionAckSet(target)).containsExactly(6L);
        cursor.delete(AckSetStateUtil.createPositionWithAckSet(
                target.getLedgerId(), target.getEntryId(), new long[] {1L}));
        assertBacklog(cursor, 11);

        cursor.resetCursor(positions.get(4));
        assertReset(cursor, positions, 4);
        assertThat(cursor.getBatchPositionAckSet(target)).isNull();
    }

    @Test
    public void testResetCountsAcknowledgmentsWhenLedgerPersistenceFallsBack() throws Exception {
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("tenant/ns/persistent/reset-fallback", config(4));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = appendEntries(ledger, ENTRY_COUNT);
        cursor.markDelete(positions.get(1));
        cursor.delete(positions.subList(4, 8));
        cursor.delete(positions.subList(10, 15));
        long metadataWrites = cursor.getStats().getPersistZookeeperSucceed();
        bkc.addEntryFailAfter(0, BKException.Code.NotEnoughBookiesException);

        cursor.resetCursor(positions.get(6));
        assertThat(cursor.getStats().getPersistZookeeperSucceed()).isGreaterThan(metadataWrites);
        assertReset(cursor, positions, 6);
        cursor.delete(positions.get(10));
        assertBacklog(cursor, 9);
    }

    @Test
    public void testFailedResetPreservesAcknowledgmentsAndCanBeRetried() throws Exception {
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("tenant/ns/persistent/reset-failure", config(4));
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        List<Position> positions = appendEntries(ledger, ENTRY_COUNT);
        cursor.markDelete(positions.get(1));
        cursor.delete(positions.subList(4, 8));
        cursor.delete(positions.get(12));
        Position oldReadPosition = cursor.getReadPosition();
        bkc.addEntryFailAfter(0, BKException.Code.NotEnoughBookiesException);
        metadataStore.setAlwaysFail(new MetadataStoreException.BadVersionException("injected reset failure"));
        try {
            assertThatThrownBy(() -> cursor.resetCursor(positions.get(6)))
                    .isInstanceOf(ManagedLedgerException.InvalidCursorPositionException.class);
            assertThat(cursor.getReadPosition()).isEqualTo(oldReadPosition);
            assertThat(cursor.getMarkDeletedPosition()).isEqualTo(positions.get(1));
            assertThat(cursor.isMessageDeleted(positions.get(4))).isTrue();
            assertThat(cursor.isMessageDeleted(positions.get(12))).isTrue();
            assertBacklog(cursor, 9);
        } finally {
            metadataStore.unsetAlwaysFail();
        }

        cursor.resetCursor(positions.get(6));
        assertReset(cursor, positions, 6);
    }

    private static ManagedLedgerConfig config(int entriesPerLedger) {
        return defaultConfig().setMaxEntriesPerLedger(entriesPerLedger)
                .setRetentionTime(-1, TimeUnit.MILLISECONDS).setRetentionSizeInMB(-1).setThrottleMarkDelete(0);
    }

    private static List<Position> appendEntries(ManagedLedgerImpl ledger, int count) throws Exception {
        List<Position> positions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            positions.add(ledger.addEntry(new byte[] {(byte) i}));
        }
        return positions;
    }

    private static void assertReset(ManagedCursorImpl cursor, List<Position> positions, int resetIndex) {
        int firstUnacknowledged = Math.max(0, resetIndex);
        Position markDeletePosition = firstUnacknowledged == 0
                ? PositionFactory.create(positions.get(0).getLedgerId(), -1)
                : positions.get(firstUnacknowledged - 1);
        assertThat(cursor.getMarkDeletedPosition()).as("mark-delete position after reset")
                .isEqualTo(markDeletePosition);
        if (resetIndex < 0) {
            assertThat(cursor.getReadPosition()).isEqualTo(markDeletePosition);
        } else if (resetIndex < positions.size()) {
            assertThat(cursor.getReadPosition()).isEqualTo(positions.get(resetIndex));
        } else {
            assertThat(cursor.getReadPosition()).isGreaterThan(positions.get(positions.size() - 1));
        }
        assertThat(cursor.getIndividuallyDeletedMessagesSet().isEmpty())
                .as("reset discards individual ACK ranges").isTrue();
        for (int i = 0; i < positions.size(); i++) {
            assertThat(cursor.isMessageDeleted(positions.get(i))).as("entry %s after reset", i)
                    .isEqualTo(i < firstUnacknowledged);
        }
        assertBacklog(cursor, positions.size() - firstUnacknowledged);
    }

    private static void assertBacklog(ManagedCursorImpl cursor, long expected) {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) cursor.getManagedLedger();
        assertThat(ledger.getEntriesAddedCounter() - cursor.getMessagesConsumedCounter())
                .as("raw counter difference without precise-backlog fallback").isEqualTo(expected);
        assertThat(cursor.getNumberOfEntriesInBacklog(false)).as("counter-based backlog").isEqualTo(expected);
        assertThat(cursor.getNumberOfEntriesInBacklog(true)).as("precise backlog").isEqualTo(expected);
        assertThat(cursor.hasBacklog(false)).as("counter-based backlog predicate").isEqualTo(expected > 0);
    }
}

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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

/**
 * Regression test for reading over a cursor that has a very large contiguous range of individually
 * deleted (acknowledged) entries while its mark-delete position is pinned by an older unacknowledged entry.
 *
 * <p>This is the shape a Key_Shared subscription ends up in when a single unacknowledged message blocks the
 * mark-delete position while consumers keep acknowledging everything after it. Reads then have to get
 * past tens of millions of already-acknowledged entries before they can fill a batch.
 *
 * <p>The skip pre-filter in {@link ManagedLedgerImpl#internalReadFromLedger} used to advance the read
 * position by the size of the scan window when every entry in that window was already acknowledged, instead of
 * hopping over the whole deleted range via
 * {@link ManagedCursorImpl#getNextAvailablePosition(Position)}. That turned a single range hop into
 * O(entries / batchSize) read-loop iterations. Each iteration re-ran
 * {@code isLedgerFullyAcked} -> {@code ManagedCursorImpl#getNumberOfEntries} ->
 * {@code RangeSetWrapper#cardinality}, which clones the cursor's per-ledger Roaring bitmap.
 */
public class ManagedCursorSkipDeletedEntriesTest extends MockedBookKeeperTestCase {

    private static final int TOTAL_ENTRIES = 20_000;
    private static final int READ_BATCH_SIZE = 100;
    /** Entries {@code [1, LAST_DELETED_INDEX]} are individually deleted, forming one contiguous range. */
    private static final int LAST_DELETED_INDEX = TOTAL_ENTRIES - 4;

    /**
     * A read that has to get past one large contiguous deleted range must hop over it rather than walk
     * it one batch at a time.
     */
    @Test(timeOut = 300_000)
    public void testReadHopsOverLargeDeletedRangeInsteadOfWalkingIt() throws Exception {
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                // Keep every entry below in a single ledger, then force a rollover so that the reads
                // under test target a closed ledger. That makes every read-loop iteration go through
                // the isLedgerFullyAcked check, as it does in production.
                .setMaxEntriesPerLedger(TOTAL_ENTRIES)
                .setRetentionTime(1, TimeUnit.HOURS)
                .setRetentionSizeInMB(-1);

        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("skip-deleted-entries", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("sub");

        List<Position> positions = new ArrayList<>(TOTAL_ENTRIES);
        for (int i = 0; i < TOTAL_ENTRIES; i++) {
            positions.add(ledger.addEntry(new byte[]{1}));
        }
        // Roll the ledger holding the entries above, so it is closed by the time it is read.
        Position afterRollover = ledger.addEntry(new byte[]{1});
        assertTrue(ledger.getLedgersInfoAsList().size() >= 2,
                "expected the entries under test to live in a closed ledger, ledgers="
                        + ledger.getLedgersInfoAsList().size());

        // Individually acknowledge [1, LAST_DELETED_INDEX]. Entry 0 stays unacknowledged and pins the
        // mark-delete position, so none of these acknowledgments can be collapsed into it.
        cursor.delete(positions.subList(1, LAST_DELETED_INDEX + 1));

        assertEquals(cursor.getMarkDeletedPosition(), PositionFactory.create(positions.get(0).getLedgerId(), -1),
                "mark-delete position must stay pinned behind the unacknowledged entry 0");
        assertEquals(cursor.getTotalNonContiguousDeletedMessagesRange(), 1,
                "the acknowledgments must collapse into a single contiguous range");

        // The deliverable entries are entry 0, the tail holes, and the entry that caused the rollover.
        List<Position> expected = new ArrayList<>();
        expected.add(positions.get(0));
        for (int i = LAST_DELETED_INDEX + 1; i < TOTAL_ENTRIES; i++) {
            expected.add(positions.get(i));
        }
        expected.add(afterRollover);

        // Count every position the skip pre-filter examines. Predicate#or evaluates this one first, so
        // it sees exactly the positions ManagedLedgerImpl walks before deciding what to read.
        AtomicInteger examinedPositions = new AtomicInteger();
        Predicate<Position> countingCondition = position -> {
            examinedPositions.incrementAndGet();
            return false;
        };

        CompletableFuture<List<Entry>> readFuture = new CompletableFuture<>();
        cursor.asyncReadEntriesWithSkip(READ_BATCH_SIZE, -1L, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                readFuture.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                readFuture.completeExceptionally(exception);
            }
        }, null, PositionFactory.LATEST, countingCondition);

        List<Entry> read = readFuture.get(120, TimeUnit.SECONDS);
        List<Position> readPositions = new ArrayList<>();
        for (Entry entry : read) {
            readPositions.add(entry.getPosition());
            entry.release();
        }
        assertEquals(readPositions, expected, "the read must return exactly the unacknowledged entries");

        // Hopping the range examines one scan window before jumping it and a small number of windows for
        // the entries actually returned. Walking it examines every position in the deleted block.
        assertTrue(examinedPositions.get() <= 4 * READ_BATCH_SIZE,
                "read examined " + examinedPositions.get() + " positions to return " + read.size()
                        + " entries over a deleted range of " + LAST_DELETED_INDEX + " entries");
    }
}

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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

/**
 * Checks {@link ManagedCursor#getNthEntry} against a real managed ledger when the backlog contains individually
 * acknowledged entries, which is what a per-subscription backlog age quantile lookup relies on.
 */
public class ManagedCursorNthEntryBacklogTest extends MockedBookKeeperTestCase {

    private static String payload(int i) {
        return "msg" + i;
    }

    @Test(timeOut = 60000)
    void nthEntryMatchesUnackedEntriesWithRandomIndividualAcks() throws Exception {
        ManagedLedger ledger = factory.open("nthEntryRandomAcks", initManagedLedgerConfig(defaultConfig()));
        ManagedCursor cursor = ledger.openCursor("c1");

        int total = 500;
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            positions.add(ledger.addEntry(payload(i).getBytes(StandardCharsets.UTF_8)));
        }

        // Leave entry 0 unacked so the mark-delete position stays put and every ack below is an individual one.
        Random random = new Random(42);
        Set<Integer> acked = new HashSet<>();
        while (acked.size() < 200) {
            int i = 1 + random.nextInt(total - 1);
            if (acked.add(i)) {
                cursor.delete(positions.get(i));
            }
        }

        List<Integer> unacked = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            if (!acked.contains(i)) {
                unacked.add(i);
            }
        }
        assertEquals(cursor.getNumberOfEntriesInBacklog(true), unacked.size());

        List<Integer> ranks = new ArrayList<>(List.of(1, 2, unacked.size() / 2, unacked.size() - 1, unacked.size()));
        for (int k = 0; k < 50; k++) {
            ranks.add(1 + random.nextInt(unacked.size()));
        }
        for (int n : ranks) {
            Entry entry = cursor.getNthEntry(n, IndividualDeletedEntries.Exclude);
            assertNotNull(entry, "rank " + n);
            assertEquals(new String(entry.getDataAndRelease(), StandardCharsets.UTF_8), payload(unacked.get(n - 1)),
                    "rank " + n);
        }

        assertNull(cursor.getNthEntry(unacked.size() + 1, IndividualDeletedEntries.Exclude));
    }

    @Test(timeOut = 60000)
    void nthEntryIsCorrectAfterMarkDeleteAdvancesOverAckedRanges() throws Exception {
        ManagedLedger ledger = factory.open("nthEntryMarkDelete", initManagedLedgerConfig(defaultConfig()));
        ManagedCursor cursor = ledger.openCursor("c1");

        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            positions.add(ledger.addEntry(payload(i).getBytes(StandardCharsets.UTF_8)));
        }

        // Ack 0..4 contiguously (mark-delete moves), then 7, 8, 12 individually.
        for (int i = 0; i <= 4; i++) {
            cursor.delete(positions.get(i));
        }
        for (int i : new int[] {7, 8, 12}) {
            cursor.delete(positions.get(i));
        }

        List<Integer> unacked = new ArrayList<>();
        for (int i = 5; i < 20; i++) {
            if (i != 7 && i != 8 && i != 12) {
                unacked.add(i);
            }
        }
        assertEquals(cursor.getNumberOfEntriesInBacklog(true), unacked.size());
        for (int n = 1; n <= unacked.size(); n++) {
            Entry entry = cursor.getNthEntry(n, IndividualDeletedEntries.Exclude);
            assertNotNull(entry, "rank " + n);
            assertEquals(new String(entry.getDataAndRelease(), StandardCharsets.UTF_8), payload(unacked.get(n - 1)),
                    "rank " + n);
        }
    }

    /**
     * Checks the lookup for every rank on backlogs that span several ledgers and have random individual acks, with and
     * without the mark-delete position moving.
     */
    @Test(timeOut = 120000)
    void nthEntryIsCorrectAcrossLedgers() throws Exception {
        for (long seed = 1; seed <= 6; seed++) {
            ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
            config.setMaxEntriesPerLedger(37);
            ManagedLedger ledger = factory.open("prop/ns/persistent/bitmapLookup" + seed, config);
            ManagedCursor cursor = ledger.openCursor("c1");
            ManagedCursorImpl impl = (ManagedCursorImpl) cursor;

            int total = 400;
            List<Position> positions = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                positions.add(ledger.addEntry(payload(i).getBytes(StandardCharsets.UTF_8)));
            }
            assertTrue(((ManagedLedgerImpl) ledger).getLedgersInfo().size() > 5);

            Random random = new Random(seed);
            // Even seeds also ack a contiguous prefix, so the mark-delete position moves into the middle.
            int firstUnacked = seed % 2 == 0 ? 1 + random.nextInt(120) : 0;
            for (int i = 0; i < firstUnacked; i++) {
                cursor.delete(positions.get(i));
            }
            double ackFraction = 0.2 + 0.7 * random.nextDouble();
            Set<Integer> acked = new HashSet<>();
            for (int i = firstUnacked + 1; i < total; i++) {
                if (random.nextDouble() < ackFraction) {
                    acked.add(i);
                    cursor.delete(positions.get(i));
                }
            }

            List<Position> unacked = new ArrayList<>();
            for (int i = firstUnacked; i < total; i++) {
                if (!acked.contains(i)) {
                    unacked.add(positions.get(i));
                }
            }
            assertEquals(cursor.getNumberOfEntriesInBacklog(true), unacked.size());
            impl.lock.readLock().lock();
            try {
                Position markDelete = impl.getMarkDeletedPosition();
                for (int n = 1; n <= unacked.size() + 1; n++) {
                    Position found = impl.getNthNotDeletedPositionAfterMarkDelete(markDelete, n);
                    if (n > unacked.size()) {
                        assertNull(found, "seed " + seed + " rank " + n);
                    } else {
                        assertEquals(found, unacked.get(n - 1), "seed " + seed + " rank " + n);
                    }
                }
            } finally {
                impl.lock.readLock().unlock();
            }
        }
    }

    private static void release(Entry entry) {
        if (entry != null) {
            entry.release();
        }
    }
}

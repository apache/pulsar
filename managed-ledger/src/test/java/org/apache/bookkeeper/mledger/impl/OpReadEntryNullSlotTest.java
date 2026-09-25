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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

/**
 * Tests for the null-slot guard in {@link OpReadEntry}: a delivered batch containing a null
 * slot (a mixed range-cache read leaves its slot null when it drops an out-of-range entry)
 * must fail the read explicitly with a proper {@link ManagedLedgerException} and release the
 * batch's buffers — instead of throwing a bare NPE in the size loop and leaking the batch.
 */
public class OpReadEntryNullSlotTest extends MockedBookKeeperTestCase {

    /** A null slot in the delivered batch is rejected explicitly and the batch is released. */
    @Test(timeOut = 20_000)
    void nullSlotFailsTheReadExplicitlyAndReleasesTheBatch() throws Exception {
        Fixture f = newFixture("opreadentry_null_slot");

        EntryImpl valid = entry(f.firstLedgerId, 0, "entry-0");
        ReadResult result = f.read(Arrays.asList(valid, null));

        ManagedLedgerException ex = result.failure.get(5, TimeUnit.SECONDS);
        assertTrue(ex.getMessage().contains("null slot"), "unexpected message: " + ex.getMessage());
        assertEquals(valid.refCnt(), 0, "the delivered batch must be released on the explicit failure");
    }

    /** A well-formed delivery still completes normally (the guard does not over-fire). */
    @Test(timeOut = 20_000)
    void validDeliveryCompletesNormally() throws Exception {
        Fixture f = newFixture("opreadentry_valid");

        EntryImpl valid = entry(f.firstLedgerId, 0, "entry-0");
        ReadResult result = f.read(List.of(valid));

        List<Entry> delivered = result.entries.get(5, TimeUnit.SECONDS);
        assertTrue(!delivered.isEmpty(), "the valid delivery must complete the read (not fail)");
        assertTrue(delivered.stream().allMatch(e -> e.getLedgerId() == f.firstLedgerId),
                "the delivered entries must come from the fixture's ledger");
        delivered.forEach(Entry::release);
    }

    // -------------------------------------------------------------------------------------------
    // Harness.
    // -------------------------------------------------------------------------------------------

    private static final class ReadResult {
        final CompletableFuture<List<Entry>> entries = new CompletableFuture<>();
        final CompletableFuture<ManagedLedgerException> failure = new CompletableFuture<>();
        final AsyncCallbacks.ReadEntriesCallback callback = new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                ReadResult.this.entries.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                ReadResult.this.failure.complete(exception);
            }
        };
    }

    private static final class Fixture {
        final ManagedCursorImpl cursor;
        final Position readPosition;
        final long firstLedgerId;

        Fixture(ManagedCursorImpl cursor, Position readPosition, long firstLedgerId) {
            this.cursor = cursor;
            this.readPosition = readPosition;
            this.firstLedgerId = firstLedgerId;
        }

        /** Deliver {@code batch} to a fresh op and return its result. */
        ReadResult read(List<Entry> batch) {
            ReadResult result = new ReadResult();
            OpReadEntry op = OpReadEntry.create(cursor, readPosition, 10, 1024 * 1024, result.callback,
                    null, PositionFactory.LATEST, null, false);
            op.readEntriesComplete(batch, null);
            return result;
        }
    }

    private Fixture newFixture(String ledgerName) throws Exception {
        ManagedLedger ledger = factory.open(ledgerName);
        Position first = ledger.addEntry("entry-0".getBytes(StandardCharsets.UTF_8));
        ledger.addEntry("entry-1".getBytes(StandardCharsets.UTF_8));
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        Position readPosition = PositionFactory.create(first.getLedgerId(), 0);
        return new Fixture((ManagedCursorImpl) cursor, readPosition, first.getLedgerId());
    }

    private static EntryImpl entry(long ledgerId, long entryId, String data) {
        return EntryImpl.create(ledgerId, entryId, data.getBytes(StandardCharsets.UTF_8));
    }
}

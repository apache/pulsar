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
import static org.testng.Assert.assertTrue;
import it.unimi.dsi.fastutil.longs.LongLongPair;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.testng.annotations.Test;

/**
 * Property tests of the aggregation of a batched skip-read, {@code BatchReadEntriesCallback}, over random
 * sequences of range reports - the entries of a range, an empty batch, a failure, a throttling - in any
 * order, interleaved with the submission of the ranges, and with a skip condition that answers on arrival
 * differently from what it answered to the scan.
 *
 * <p>The ranges and the holes are the ones that {@code buildRanges} built from a random skip pattern. The
 * sequence is played against a real managed ledger and cursor, and the operation carries the scan's
 * condition, so that a continuation read skips the same entries.
 *
 * <p>What is checked does not restate the aggregation's logic. The operation reports once. The entries that
 * were delivered by then, and by the reads that follow until the cursor is at the end of its entries, are all
 * different - no entry is delivered twice. And there is a position {@code P} of the window such that the
 * entries delivered before it are a prefix of what the batched read had to deliver - the entries of its
 * ranges that are neither a hole nor skipped on arrival - and the entries delivered from it on are exactly
 * every entry from it on that the condition does not skip. {@code P} is where the operation's read position
 * was after the batched read: everything after it is read again, everything before it is what the batched
 * read delivered. A batch that completed while a range was still in flight delivers entries past the entries
 * of that range that it did not deliver, which no {@code P} can account for.
 *
 * <p>Every entry buffer is released.
 */
public class BatchedSkipReadCallbackPropertyTest extends MockedBookKeeperTestCase {

    private static final int MAX_RANGES = 4;

    /** What a range read reports. */
    private enum Report {
        /** The entries of the range. */
        ENTRIES,
        /** An empty batch. */
        EMPTY,
        /** A failure that is not a throttling one. */
        FAILURE,
        /** A throttling by the inflight reads limiter. */
        THROTTLED
    }

    /** The batched read of one sequence, and what was observed of it. */
    private final class Sequence {
        final List<LongLongPair> ranges = new ArrayList<>();
        final List<LongLongPair> holes = new ArrayList<>();
        final BitSet skippedAtScan = new BitSet();
        /** The entries the condition skips when they arrive: its own answer, not the scan's. */
        final BitSet skippedOnArrival = new BitSet();
        final int entryCount;
        final long ledgerId;
        final ManagedLedgerImpl ledger;
        final ManagedCursorImpl cursor;
        final Report[] reports;
        final boolean[] reported;
        /** The entry buffers the sequence created for the ranges' reports. */
        final List<EntryImpl> created = new ArrayList<>();
        /** Every entry a report delivered, to release its buffer as a reader does. */
        final List<Entry> deliveredEntries = new ArrayList<>();
        /** The entry ids every report delivered, in order. */
        final List<Long> allDelivered = new ArrayList<>();
        final CountDownLatch operationReported = new CountDownLatch(1);
        final List<String> events = new ArrayList<>();
        final LongPredicate shouldSkipEntry;

        Sequence(Random random, int index) throws Exception {
            entryCount = random.nextInt(6, 30);
            // A skip pattern with runs, as individual acknowledgements and delayed messages leave behind
            double skipProbability = 0.15 + random.nextDouble() * 0.4;
            for (int entryId = 0; entryId < entryCount; entryId++) {
                if (random.nextDouble() < skipProbability) {
                    skippedAtScan.set(entryId);
                }
            }
            ManagedLedgerImpl.buildRanges(0, entryCount - 1, entryId -> skippedAtScan.get((int) entryId),
                    MAX_RANGES, ranges, random.nextInt(0, 10), random.nextInt(1, 30), holes);
            ledger = (ManagedLedgerImpl) factory.open("batched-callback-" + index,
                    initManagedLedgerConfig(defaultConfig()));
            for (int entryId = 0; entryId < entryCount; entryId++) {
                ledger.addEntry(new byte[8]);
            }
            cursor = (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);
            ledgerId = ledger.currentLedger.getId();
            reports = new Report[ranges.size()];
            for (int i = 0; i < ranges.size(); i++) {
                reports[i] = switch (random.nextInt(10)) {
                    case 0, 1 -> Report.FAILURE;
                    case 2 -> Report.THROTTLED;
                    case 3 -> Report.EMPTY;
                    default -> Report.ENTRIES;
                };
            }
            reported = new boolean[ranges.size()];
            // The condition when the entries arrive is drawn on its own, so that it disagrees with the
            // scan in both ways. An entry the scan read as readable can be skipped by then, as one
            // acknowledged while the reads were in flight. An entry the scan read as skipped - of a
            // merged hole, or in a gap between two ranges - can be readable by then, as a delayed
            // message whose delivery time passed while the reads were in flight.
            for (int entryId = 0; entryId < entryCount; entryId++) {
                if (random.nextBoolean()) {
                    skippedOnArrival.set(entryId);
                }
            }
            shouldSkipEntry = entryId -> skippedOnArrival.get((int) entryId);
        }

        long expectedCount() {
            long count = 0;
            for (LongLongPair range : ranges) {
                count += range.rightLong() - range.leftLong() + 1;
            }
            for (LongLongPair hole : holes) {
                count -= hole.rightLong() - hole.leftLong() + 1;
            }
            return count;
        }

        boolean isHole(long entryId) {
            for (LongLongPair hole : holes) {
                if (hole.leftLong() <= entryId && entryId <= hole.rightLong()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * What the batched read had to deliver, in the order of the ranges: the entries of its ranges
         * that are neither a hole nor skipped on arrival. The holes are released by the scan's result,
         * whatever the condition says when they arrive.
         */
        List<Long> deliverable() {
            List<Long> entries = new ArrayList<>();
            for (LongLongPair range : ranges) {
                for (long entryId = range.leftLong(); entryId <= range.rightLong(); entryId++) {
                    if (!isHole(entryId) && !skippedOnArrival.get((int) entryId)) {
                        entries.add(entryId);
                    }
                }
            }
            return entries;
        }

        /** Plays the sequence: the submission of the ranges, with their reports interleaved. */
        void play(Random random) {
            ReadEntriesCallback recording = new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    List<Long> ids = new ArrayList<>();
                    for (Entry entry : entries) {
                        ids.add(entry.getEntryId());
                        deliveredEntries.add(entry);
                        allDelivered.add(entry.getEntryId());
                    }
                    events.add("a report delivers " + ids + ", the read position is " + cursor.getReadPosition());
                    operationReported.countDown();
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    events.add("a report fails with " + exception.getClass().getSimpleName());
                    operationReported.countDown();
                }
            };
            // The operation reads the whole window, as a dispatcher's read of a backlog does: when the
            // batched read stops short of it, the operation goes on reading with the same condition.
            OpReadEntry opReadEntry = OpReadEntry.create(cursor, cursor.readPosition, entryCount, -1, recording,
                    null,
                    ledger.lastConfirmedEntry,
                    position -> position.getLedgerId() == ledgerId
                            && skippedOnArrival.get((int) position.getEntryId()), true);
            long lastReadPosition = ranges.get(ranges.size() - 1).rightLong();
            ManagedLedgerImpl.BatchReadEntriesCallback callback = new ManagedLedgerImpl.BatchReadEntriesCallback(
                    ranges, expectedCount(), opReadEntry, PositionFactory.create(ledgerId, lastReadPosition), null,
                    shouldSkipEntry, holes);

            // The ranges are submitted in order. A range already submitted reports at random points of
            // the submission, each one at most once.
            List<Integer> unreported = new ArrayList<>();
            for (int i = 0; i < ranges.size(); i++) {
                if (callback.shouldStopSubmission()) {
                    events.add("the submission stops before range " + i);
                    break;
                }
                callback.onRangeSubmitted();
                unreported.add(i);
                while (!unreported.isEmpty() && random.nextInt(3) == 0) {
                    report(callback, unreported.remove(random.nextInt(unreported.size())));
                }
            }
            callback.submissionFinished();
            // A range that was never submitted, because the submission stopped, does not report.
            while (!unreported.isEmpty()) {
                report(callback, unreported.remove(random.nextInt(unreported.size())));
            }
            // The operation reports once the batched read is done with it: with the entries delivered,
            // or after a read that went on because none were.
            try {
                assertTrue(operationReported.await(10, TimeUnit.SECONDS), "the read must report: " + describe());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void report(ManagedLedgerImpl.BatchReadEntriesCallback callback, int index) {
            assertTrue(!reported[index], "a range reports once");
            reported[index] = true;
            LongLongPair range = ranges.get(index);
            events.add("range " + index + " reports " + reports[index]);
            switch (reports[index]) {
                case ENTRIES:
                    List<Entry> entries = new ArrayList<>();
                    for (long entryId = range.leftLong(); entryId <= range.rightLong(); entryId++) {
                        EntryImpl entry = EntryImpl.create(ledgerId, entryId, new byte[8]);
                        created.add(entry);
                        entries.add(entry);
                    }
                    callback.readEntriesComplete(entries, range);
                    break;
                case EMPTY:
                    callback.readEntriesComplete(List.of(), range);
                    break;
                case FAILURE:
                    callback.readEntriesFailed(
                            new ManagedLedgerException.InvalidCursorPositionException("failed"), range);
                    break;
                default:
                    callback.readEntriesFailed(new TooManyRequestsException("throttled"), range);
            }
        }

        /**
         * Reads with a skip condition and waits for the report. A report with no entries makes the
         * operation read on, so the read reports with its entries, or fails.
         */
        List<Entry> readWithSkip(long fromEntry) throws Exception {
            Predicate<Position> condition = position -> position.getLedgerId() == ledgerId
                    && skippedOnArrival.get((int) position.getEntryId());
            CompletableFuture<List<Entry>> future = new CompletableFuture<>();
            cursor.asyncReadEntriesWithSkip(entryCount, -1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    future.complete(entries);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST, condition);
            return future.get(10, TimeUnit.SECONDS);
        }

        String describe() {
            return "ranges=" + ranges + " holes=" + holes + " reports=" + List.of(reports)
                    + " skippedAtScan=" + skippedAtScan + " skippedOnArrival=" + skippedOnArrival
                    + " deliverable=" + deliverable() + " allDelivered=" + allDelivered + " events=" + events;
        }
    }

    /**
     * Plays random sequences, and checks what the batched read and the reads after it delivered against
     * the property above: the entries delivered once, and accounted for by a read position {@code P}.
     */
    @Test
    public void testDeliversAPrefixAndRereadsTheRest() throws Exception {
        Random random = new Random(22566);
        int played = 0;
        int withHoles = 0;
        int withoutHoles = 0;
        int withFailure = 0;
        int capped = 0;
        for (int iteration = 0; iteration < 4_000; iteration++) {
            Sequence sequence = new Sequence(random, iteration);
            // a single range with no hole in it takes the sequential read path, not the batched one
            if (sequence.ranges.size() < 2 && sequence.holes.isEmpty()) {
                sequence.ledger.close();
                continue;
            }
            played++;
            if (sequence.holes.isEmpty()) {
                withoutHoles++;
            } else {
                withHoles++;
            }
            if (sequence.ranges.size() == MAX_RANGES) {
                capped++;
            }
            boolean failed = false;
            for (Report report : sequence.reports) {
                failed |= report != Report.ENTRIES;
            }
            if (failed) {
                withFailure++;
            }
            sequence.play(random);
            check(sequence);
            sequence.ledger.close();
        }
        // The sequences must have exercised the interesting cases
        String covered = "sequences played: " + played + ", with holes: " + withHoles + ", without holes: "
                + withoutHoles + ", with a failed, throttled or empty range: " + withFailure
                + ", at the range cap: " + capped;
        assertTrue(played >= 2500, covered);
        assertTrue(withHoles >= 600, covered);
        assertTrue(withoutHoles >= 600, covered);
        assertTrue(withFailure >= 800, covered);
        assertTrue(capped >= 600, covered);
    }

    private void check(Sequence sequence) throws Exception {
        String context = sequence.describe();
        // The read after the batched read: the same cursor, with the same condition, until a read
        // delivers no entry - the cursor has entries as long as some are not acknowledged, which the
        // condition alone skips.
        int reads = 0;
        while (true) {
            List<Entry> entries = sequence.readWithSkip(0);
            if (entries.isEmpty()) {
                break;
            }
            // guard against a read loop that does not advance
            assertTrue(++reads < 50, "the reads must reach the end: " + context);
        }
        // The reads must take the cursor to the end of the window: no entry the condition reads is
        // left behind the read position.
        assertEquals(sequence.cursor.getReadPosition().getEntryId(), (long) sequence.entryCount, context);

        // No entry is delivered twice
        assertEquals(new HashSet<>(sequence.allDelivered).size(), sequence.allDelivered.size(),
                "an entry must not be delivered twice; " + context);
        // ... and in order within a report: the entries of a report are sorted by position
        Set<Long> delivered = new TreeSet<>(sequence.allDelivered);

        // There is a read position P of the batched read that accounts for what was delivered: the
        // entries delivered before it are a prefix of what the batched read had to deliver, and the
        // entries delivered from it on are exactly the entries from it on that the condition skips.
        List<Long> deliverable = sequence.deliverable();
        String noPosition = "no read position accounts for the entries delivered; deliverable=" + deliverable
                + " vs delivered=" + delivered + "; " + context;
        boolean accounted = false;
        for (int position = 0; position <= sequence.entryCount && !accounted; position++) {
            // the reads after the batched read deliver every entry from the position on that the
            // condition does not skip
            Set<Long> reread = new TreeSet<>();
            for (long entryId = position; entryId < sequence.entryCount; entryId++) {
                if (!sequence.skippedOnArrival.get((int) entryId)) {
                    reread.add(entryId);
                }
            }
            // what the batched read delivered: the entries delivered before the position. Every entry
            // it delivered lies in a range, before the position, or it would have been read again.
            Set<Long> before = new TreeSet<>(delivered);
            before.removeAll(reread);
            for (long entryId : before) {
                if (entryId >= position) {
                    before = null;
                    break;
                }
            }
            if (before == null) {
                continue;
            }
            // and delivered is exactly before + reread
            Set<Long> union = new TreeSet<>(before);
            union.addAll(reread);
            if (!union.equals(delivered)) {
                continue;
            }
            // before is a prefix of the deliverable entries, in their order
            int prefixLength = 0;
            while (prefixLength < deliverable.size() && before.contains(deliverable.get(prefixLength))) {
                prefixLength++;
            }
            if (prefixLength == before.size()) {
                accounted = true;
            }
        }
        assertTrue(accounted, noPosition);

        // A reader releases the entries it was delivered; every buffer must be released then - the
        // delivered ones, and the ones the aggregation dropped.
        for (Entry entry : sequence.deliveredEntries) {
            entry.release();
        }
        for (EntryImpl entry : sequence.created) {
            assertEquals(entry.refCnt(), 0, "an entry buffer must be released: " + context);
        }
    }
}

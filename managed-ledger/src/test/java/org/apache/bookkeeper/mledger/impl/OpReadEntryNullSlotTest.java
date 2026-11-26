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
import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.NO_MAX_SIZE_LIMIT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.cache.InflightReadsLimiter;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheManagerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Tests for the null-slot guard in {@link OpReadEntry}: a delivered batch containing a null
 * slot (a mixed range-cache read leaves a slot unfilled when the storage leg answers short of
 * the missing range) must fail the read explicitly with a proper {@link ManagedLedgerException}
 * and release the batch's buffers — instead of throwing a bare NPE in the size loop and
 * leaking the batch.
 * Discarding the batch must not count as reads on the shared expected-read count of the cached
 * entries either: the entries were never delivered to a reader.
 *
 * <p>The second half covers the callbacks wrapped around the original one: the in-flight reads
 * limiter's wrapper ({@code RangeEntryCacheImpl.doAsyncReadEntriesWithAcquiredPermits}) iterates
 * the batch before the original callback runs, the read-timeout wrapper
 * ({@link ManagedLedgerImpl.ReadEntryCallbackWrapper}) releases the batch when it arrives late,
 * and the replay range read of {@code ManagedCursorImpl.readReplayRange} delivers each wanted
 * entry on its own. A null slot must not break the permit accounting or the delivery on any of
 * these paths — the callback still runs and every permit is returned.
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

    /**
     * The discarded siblings of a null slot were never delivered to a reader: their release must
     * not count as a read on the shared expected-read count, or the cached entries would become
     * eligible for eviction before their expected readers got to them.
     */
    @Test(timeOut = 20_000)
    void nullSlotDiscardDoesNotDecrementTheExpectedReadCount() throws Exception {
        Fixture f = newFixture("opreadentry_null_slot_read_count");

        // A cache-reader copy shares the cached entry's read-count handler.
        EntryImpl cached = entry(f.firstLedgerId, 0, "entry-0", 1);
        EntryImpl deliveredCopy = EntryImpl.create(cached);
        ReadResult result = f.read(Arrays.asList(deliveredCopy, null));

        ManagedLedgerException ex = result.failure.get(5, TimeUnit.SECONDS);
        assertTrue(ex.getMessage().contains("null slot"), "unexpected message: " + ex.getMessage());
        assertEquals(deliveredCopy.refCnt(), 0, "the delivered batch must be released on the explicit failure");
        assertEquals(cached.getReadCountHandler().getExpectedReadCount(), 1,
                "a discarded delivery must not count as a read");
        cached.release();
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

    /**
     * A storage leg answering short, delivered through {@link RangeEntryCacheImpl} with the
     * in-flight reads limiter enabled: the wrapper must deliver the re-read's batch to the
     * original callback (a bare NPE there would leave the read callback uncompleted, the valid
     * entries unreleased and the acquired permit leaked) and every permit must be returned once
     * the delivered batch's entries are released. Before the re-read at the source, the short
     * leg surfaced as a null slot delivered through the wrapper, whose immediate share return
     * that path exercised.
     */
    @Test(timeOut = 20_000)
    void shortLegThroughRangeCacheWithLimiterIsRecoveredAndPermitsFullyReleased() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(100_000);
        // Keep the primed entries in the cache for the whole test (TTL eviction otherwise clears them).
        factoryConfig.setCacheEvictionIntervalMs(3600_000);
        ManagedLedgerFactoryImpl limiterFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            // Batch read disabled: its contiguity validation rejects short results, while the plain
            // readUnconfirmedAsync leg is where a mixed read can leave a slot unfilled.
            ManagedLedgerConfig config = defaultConfig();
            config.setBatchReadEnabled(false);
            ManagedLedgerImpl ml = (ManagedLedgerImpl) limiterFactory.open("opreadentry_null_slot_limiter", config);
            for (int i = 0; i < 5; i++) {
                ml.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
            }
            RangeEntryCacheImpl cache = (RangeEntryCacheImpl) ml.entryCache;
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) limiterFactory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();

            // Prime the cache with a partial range [0..1] so the main read is a MIXED read.
            cache.clear();
            primeCache(cache, ml.currentLedger, 0, 1);
            Awaitility.await().untilAsserted(() ->
                    assertEquals(limiter.getRemainingBytes(), totalCapacity, "priming read permits must return"));
            assertTrue(cache.getSize() > 0, "the priming read must have cached entries 0 and 1");

            // Storage leg of the mixed read [0..4]: return a SHORT result [2,3] for the missing
            // range [2..4], as a bookie that has fewer entries than the range asks for. The short
            // leg makes the mixed read discard what it collected and re-read the whole range
            // [0..4], which the storage behind the spy answers completely — so the wrapper
            // delivers a clean batch with no null slot. The entries list must be mutable:
            // readFromStorage closes the LedgerEntries, whose recycle clears the list.
            long ledgerId = ml.currentLedger.getId();
            LedgerHandle spyLedger = Mockito.spy(ml.currentLedger);
            Mockito.doAnswer(invocation -> CompletableFuture.completedFuture(LedgerEntriesImpl.create(
                    new ArrayList<>(List.of(ledgerEntry(ledgerId, 2), ledgerEntry(ledgerId, 3))))))
                    .when(spyLedger).readUnconfirmedAsync(2L, 4L);

            ReadResult result = new ReadResult();
            cache.asyncReadEntry(spyLedger, 0, 4, NO_MAX_SIZE_LIMIT, () -> 1, result.callback, new Object());

            List<Entry> delivered = result.entries.get(5, TimeUnit.SECONDS);
            assertEquals(delivered.size(), 5, "the re-read batch must be delivered with its full range size");
            for (int i = 0; i < 5; i++) {
                assertEquals(delivered.get(i).getEntryId(), i, "the re-read batch must carry every entry");
            }
            // the short leg made the mixed read re-read the whole range at the source
            Mockito.verify(spyLedger).readUnconfirmedAsync(0L, 4L);
            delivered.forEach(Entry::release);
            Awaitility.await().untilAsserted(() ->
                    assertEquals(limiter.getRemainingBytes(), totalCapacity, "every permit must be returned"));
        } finally {
            limiterFactory.shutdown();
        }
    }

    /**
     * A sparse batch delivered after the read timeout fired: the late-completion cleanup of
     * {@link ManagedLedgerImpl.ReadEntryCallbackWrapper} must discard every valid slot — a plain
     * {@code forEach(Entry::release)} stops at the null slot, leaking the entries after it
     * together with their share of the in-flight read permits.
     */
    @Test(timeOut = 30_000)
    void lateSparseBatchAfterReadTimeoutIsFullyDiscardedAndPermitsRestored() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(100_000);
        // Keep the primed entries in the cache for the whole test (TTL eviction otherwise clears them).
        factoryConfig.setCacheEvictionIntervalMs(3600_000);
        ManagedLedgerFactoryImpl limiterFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerConfig config = defaultConfig();
            config.setBatchReadEnabled(false);
            ManagedLedgerImpl ml = (ManagedLedgerImpl) limiterFactory.open("opreadentry_null_slot_timeout", config);
            for (int i = 0; i < 5; i++) {
                ml.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
            }
            RangeEntryCacheImpl cache = (RangeEntryCacheImpl) ml.entryCache;
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) limiterFactory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();

            // Prime the cache with [0..0] and [3..4], leaving the middle range [1..2] missing:
            // the mixed read [0..4] asks storage for [1..2], and the read times out while that
            // leg is in flight.
            cache.clear();
            primeCache(cache, ml.currentLedger, 0, 0);
            primeCache(cache, ml.currentLedger, 3, 4);
            Awaitility.await().untilAsserted(() ->
                    assertEquals(limiter.getRemainingBytes(), totalCapacity, "priming read permits must return"));

            long ledgerId = ml.currentLedger.getId();
            LedgerHandle spyLedger = Mockito.spy(ml.currentLedger);
            CompletableFuture<LedgerEntries> lateStorage = new CompletableFuture<>();
            Mockito.doAnswer(invocation -> lateStorage).when(spyLedger).readUnconfirmedAsync(1L, 2L);

            // The read is wrapped with the read-timeout callback, and the timeout completes
            // before the storage leg answers.
            ReadResult result = new ReadResult();
            ManagedLedgerImpl.ReadEntryCallbackWrapper timeoutWrapper =
                    ManagedLedgerImpl.ReadEntryCallbackWrapper.create(ml, ledgerId, 0, result.callback,
                            null, System.nanoTime());
            cache.asyncReadEntry(spyLedger, 0, 4, NO_MAX_SIZE_LIMIT, () -> 1, timeoutWrapper, new Object());
            timeoutWrapper.registerTimeout();
            timeoutWrapper.triggerReadTimeout(new ManagedLedgerException("read timed out"));
            result.failure.get(5, TimeUnit.SECONDS);

            // The leg answers short with only entry 1: the mixed read rejects it, discards what
            // it collected and re-reads the whole range, which the real ledger answers in full —
            // a late batch the timeout wrapper must discard unread. The entries list must be
            // mutable: readFromStorage closes the LedgerEntries, whose recycle clears the list.
            lateStorage.complete(LedgerEntriesImpl.create(new ArrayList<>(List.of(ledgerEntry(ledgerId, 1)))));

            Awaitility.await().untilAsserted(() ->
                    assertEquals(limiter.getRemainingBytes(), totalCapacity,
                            "every in-flight read permit must be returned"));
            assertTrue(!result.entries.isDone(), "the late batch must not be delivered after the timeout");

            // A late batch may still carry null slots, as a mixed read left them before the
            // source-side recovery: injected directly, since the recovery now keeps such a batch
            // from being delivered, the cleanup's discard must tolerate them and release the
            // entries after the null slot.
            List<Entry> sparseBatch = new ArrayList<>();
            EntryImpl lateEntry1 = EntryImpl.create(ledgerId, 1, Unpooled.EMPTY_BUFFER);
            EntryImpl lateEntry3 = EntryImpl.create(ledgerId, 3, Unpooled.EMPTY_BUFFER);
            sparseBatch.add(lateEntry1);
            sparseBatch.add(null);
            sparseBatch.add(lateEntry3);
            timeoutWrapper.readEntriesComplete(sparseBatch, null);
            assertEquals(lateEntry1.refCnt(), 0, "the late batch's first entry must be discarded");
            assertEquals(lateEntry3.refCnt(), 0, "the entry after the null slot must be discarded");
            assertTrue(!result.entries.isDone(), "the late sparse batch must not be delivered after the timeout");
        } finally {
            limiterFactory.shutdown();
        }
    }

    /**
     * A replay range read whose storage answers short ({@code ManagedCursorImpl.readReplayRange}): the
     * wanted position the delivered batch omits fails the replay as its own read would, while the
     * wanted entry the read did return and the in-between entries are released and every in-flight
     * read permit is returned. A short whole-range re-read of a mixed read now fails the read before
     * it reaches the replay's callback ({@code RangeEntryCacheImplTest}); the plain short answer of a
     * full miss keeps reaching it.
     */
    @Test(timeOut = 30_000)
    void shortReplayRangeReadFailsTheMissingPositionAndReleasesTheRest() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(100_000);
        // Keep the primed entries in the cache for the whole test (TTL eviction otherwise clears them).
        factoryConfig.setCacheEvictionIntervalMs(3600_000);
        ManagedLedgerFactoryImpl limiterFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            // Batch read disabled: its contiguity validation rejects short results, while the plain
            // readUnconfirmedAsync leg is where a mixed read can leave a slot unfilled.
            ManagedLedgerConfig config = defaultConfig();
            config.setBatchReadEnabled(false);
            ManagedLedgerImpl ml = (ManagedLedgerImpl) limiterFactory.open("opreadentry_null_slot_replay", config);
            for (int i = 0; i < 6; i++) {
                ml.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
            }
            RangeEntryCacheImpl cache = (RangeEntryCacheImpl) ml.entryCache;
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) limiterFactory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            long ledgerId = ml.currentLedger.getId();

            // The replay positions 0 and 5 are cache misses, grouped into one range read [0..5],
            // which nothing primes: a full miss, whose storage answer reaches the replay's callback
            // as the plain read path delivers it.
            cache.clear();
            Awaitility.await().untilAsserted(() ->
                    assertEquals(limiter.getRemainingBytes(), totalCapacity, "priming read permits must return"));

            ManagedCursor cursor = ml.newNonDurableCursor(PositionFactory.EARLIEST);

            // Storage answers the range read [0..5] SHORT — entries 0 to 4 without 5, as a bookie
            // that has fewer entries than the range asks for — so the replay's wanted position 5
            // is absent from the delivered batch. The entries list must be mutable:
            // readFromStorage closes the LedgerEntries, whose recycle clears the list.
            LedgerHandle spyLedger = Mockito.spy(ml.currentLedger);
            Mockito.doAnswer(invocation -> CompletableFuture.completedFuture(LedgerEntriesImpl.create(
                    new ArrayList<>(List.of(ledgerEntry(ledgerId, 0), ledgerEntry(ledgerId, 1),
                            ledgerEntry(ledgerId, 2), ledgerEntry(ledgerId, 3), ledgerEntry(ledgerId, 4))))))
                    .when(spyLedger).readUnconfirmedAsync(0L, 5L);
            ml.currentLedger = spyLedger;

            ReadResult result = new ReadResult();
            cursor.asyncReplayEntries(
                    Set.of(PositionFactory.create(ledgerId, 0), PositionFactory.create(ledgerId, 5)),
                    result.callback, new Object(), false);

            ManagedLedgerException ex = result.failure.get(5, TimeUnit.SECONDS);
            assertTrue(ex.getMessage().contains("Entry not found in the replay read"),
                    "unexpected message: " + ex.getMessage());
            assertTrue(!result.entries.isDone(), "the replay must fail, not deliver");
            // the range read [0..5] went to storage and answered short
            Mockito.verify(spyLedger).readUnconfirmedAsync(0L, 5L);
            // Every permit of the read returns: the shares of the delivered wanted entry and of
            // the in-between entries on their release.
            Awaitility.await().untilAsserted(() ->
                    assertEquals(limiter.getRemainingBytes(), totalCapacity, "every permit must be returned"));
        } finally {
            limiterFactory.shutdown();
        }
    }

    /**
     * The null-slot skip of {@code ManagedCursorImpl.readReplayRange}: the mixed read now rejects
     * incomplete answers before delivery, so a slot the range read left unfilled cannot reach the
     * replay's callback through the pipeline anymore — this injects one at the range read itself,
     * keeping the guard covered as defense in depth. The null slot is skipped, the wanted position
     * the batch omits fails the replay as its own read would, and every entry the batch did
     * deliver is released.
     */
    @Test(timeOut = 30_000)
    void nullSlotInReplayRangeReadIsSkippedAndTheMissingPositionFails() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(100_000);
        ManagedLedgerFactoryImpl limiterFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerConfig config = defaultConfig();
            config.setBatchReadEnabled(false);
            ManagedLedgerImpl ml = (ManagedLedgerImpl) limiterFactory.open("opreadentry_null_slot_replay_null", config);
            for (int i = 0; i < 6; i++) {
                ml.addEntry(("entry-" + i).getBytes(StandardCharsets.UTF_8));
            }
            long ledgerId = ml.currentLedger.getId();

            // The range read delivers [0..3, null] for [0..5]: the null slot is entry 4's, and the
            // wanted position 5 the batch never carries.
            List<EntryImpl> batchEntries = new ArrayList<>();
            List<Entry> sparse = new ArrayList<>();
            for (long entryId = 0; entryId <= 3; entryId++) {
                EntryImpl entry = entry(ledgerId, entryId, "entry-" + entryId);
                batchEntries.add(entry);
                sparse.add(entry);
            }
            sparse.add(null);
            ManagedLedgerImpl spyMl = Mockito.spy(ml);
            Mockito.doAnswer(invocation -> {
                AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(5);
                callback.readEntriesComplete(sparse, invocation.getArgument(6));
                return null;
            }).when(spyMl).asyncReadEntries(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(),
                    Mockito.anyInt(), Mockito.anyInt(), Mockito.any(), Mockito.any());

            ManagedCursor cursor = spyMl.newNonDurableCursor(PositionFactory.EARLIEST);
            ReadResult result = new ReadResult();
            cursor.asyncReplayEntries(
                    Set.of(PositionFactory.create(ledgerId, 0), PositionFactory.create(ledgerId, 5)),
                    result.callback, new Object(), false);

            ManagedLedgerException ex = result.failure.get(5, TimeUnit.SECONDS);
            assertTrue(ex.getMessage().contains("Entry not found in the replay read"),
                    "unexpected message: " + ex.getMessage());
            assertTrue(!result.entries.isDone(), "the replay must fail, not deliver");
            for (EntryImpl batchEntry : batchEntries) {
                assertEquals(batchEntry.refCnt(), 0, "every entry the batch delivered must be released");
            }
        } finally {
            limiterFactory.shutdown();
        }
    }

    private static void primeCache(RangeEntryCacheImpl cache, LedgerHandle lh, long firstEntry, long lastEntry)
            throws Exception {
        ReadResult prime = new ReadResult();
        cache.asyncReadEntry(lh, firstEntry, lastEntry, NO_MAX_SIZE_LIMIT, () -> 1, prime.callback, new Object());
        prime.entries.get(5, TimeUnit.SECONDS).forEach(Entry::release);
    }

    private static LedgerEntry ledgerEntry(long ledgerId, long entryId) {
        byte[] data = ("entry-" + entryId).getBytes(StandardCharsets.UTF_8);
        return LedgerEntryImpl.create(ledgerId, entryId, data.length, Unpooled.wrappedBuffer(data));
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

    private static EntryImpl entry(long ledgerId, long entryId, String data, int expectedReadCount) {
        return EntryImpl.create(ledgerId, entryId, data.getBytes(StandardCharsets.UTF_8), expectedReadCount);
    }
}

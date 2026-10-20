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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
 * the batch before the original callback runs, and the read-timeout wrapper
 * ({@link ManagedLedgerImpl.ReadEntryCallbackWrapper}) releases the batch when it arrives late.
 * A null slot must not break the permit accounting on either path — the batch still reaches the
 * callback and every permit is returned.
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
     * A null slot delivered through {@link RangeEntryCacheImpl} with the in-flight reads limiter
     * enabled: the wrapper must deliver the batch to the original callback (a bare NPE there
     * would leave the read callback uncompleted, the valid entries unreleased and the acquired
     * permit leaked) and every permit must be returned once the delivered entries are handled —
     * the null slot's share immediately, the valid entries' share on their release.
     */
    @Test(timeOut = 20_000)
    void nullSlotThroughRangeCacheWithLimiterIsDeliveredAndPermitsFullyReleased() throws Exception {
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
            // range [2..4], leaving slot 4 null in the assembled batch. The entries list must be
            // mutable: readFromStorage closes the LedgerEntries, whose recycle clears the list.
            long ledgerId = ml.currentLedger.getId();
            LedgerHandle spyLedger = Mockito.spy(ml.currentLedger);
            Mockito.doAnswer(invocation -> CompletableFuture.completedFuture(LedgerEntriesImpl.create(
                    new ArrayList<>(List.of(ledgerEntry(ledgerId, 2), ledgerEntry(ledgerId, 3))))))
                    .when(spyLedger).readUnconfirmedAsync(2L, 4L);

            ReadResult result = new ReadResult();
            cache.asyncReadEntry(spyLedger, 0, 4, NO_MAX_SIZE_LIMIT, () -> 1, result.callback, new Object());

            List<Entry> delivered = result.entries.get(5, TimeUnit.SECONDS);
            assertEquals(delivered.size(), 5, "the mixed-read batch must be delivered with its full range size");
            assertNull(delivered.get(4), "the unfilled slot must be delivered as null");
            assertEquals(delivered.stream().filter(Objects::nonNull).count(), 4,
                    "the other slots must hold the cached and read entries");
            delivered.stream().filter(Objects::nonNull).forEach(Entry::release);
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
            // the mixed read [0..4] assembles [cached, stored, null, cached, cached] when the
            // storage leg answers short with only entry 1 — the null slot sits in the middle,
            // so a cleanup stopping at it would leak the entries after it.
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

            // The sparse batch arrives late: every valid slot must be discarded and every share
            // of the in-flight read permits must be returned. The entries list must be mutable:
            // readFromStorage closes the LedgerEntries, whose recycle clears the list.
            lateStorage.complete(LedgerEntriesImpl.create(new ArrayList<>(List.of(ledgerEntry(ledgerId, 1)))));

            Awaitility.await().untilAsserted(() ->
                    assertEquals(limiter.getRemainingBytes(), totalCapacity,
                            "every in-flight read permit must be returned"));
            assertTrue(!result.entries.isDone(), "the late batch must not be delivered after the timeout");
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

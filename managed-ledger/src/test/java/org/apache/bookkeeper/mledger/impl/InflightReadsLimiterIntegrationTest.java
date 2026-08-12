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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheDisabled;
import org.apache.bookkeeper.mledger.impl.cache.InflightReadsLimiter;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheImpl;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheManagerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class InflightReadsLimiterIntegrationTest extends MockedBookKeeperTestCase {

    @Test
    public void testCacheDisabledReadsUseInflightReadsLimiter() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[] {1});
            EntryCache entryCache = ml.entryCache;
            Assert.assertTrue(entryCache instanceof EntryCacheDisabled);

            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) factory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            CompletableFuture<List<Entry>> entriesFuture = new CompletableFuture<>();
            entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entriesFuture.complete(entries);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    entriesFuture.completeExceptionally(exception);
                }
            }, new Object());

            List<Entry> entries = entriesFuture.join();
            long expectedReadSize = Math.max(1,
                    ml.currentLedger.getLength() / (ml.currentLedger.getLastAddConfirmed() + 1))
                    + RangeEntryCacheImpl.BOOKKEEPER_READ_OVERHEAD_PER_ENTRY;
            Awaitility.await().untilAsserted(() ->
                    Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity - expectedReadSize));
            entries.forEach(Entry::release);
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledMultiEntryReadUsesExactInflightReadsPermits() throws Exception {
        final int entrySize = 100;
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_multi_entry",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[entrySize]);
            ml.addEntry(new byte[entrySize]);
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) factory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            HoldingReadEntriesCallback callback = new HoldingReadEntriesCallback();

            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 1, () -> 0, callback, new Object());

            List<Entry> entries = callback.entries.join();
            long expectedReadSize = 2L * (entrySize + RangeEntryCacheImpl.BOOKKEEPER_READ_OVERHEAD_PER_ENTRY);
            Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity - expectedReadSize);
            entries.forEach(Entry::release);
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledReadCallbackFailureDoesNotReleasePermitsTwice() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_callback_failure",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[] {1});
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) factory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            CompletableFuture<Void> readCompleted = new CompletableFuture<>();
            AtomicInteger failedCallbacks = new AtomicInteger();

            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entries.forEach(Entry::release);
                    readCompleted.complete(null);
                    throw new RuntimeException("Expected callback failure");
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    failedCallbacks.incrementAndGet();
                }
            }, new Object());

            readCompleted.join();
            Awaitility.await().pollDelay(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
                Assert.assertEquals(failedCallbacks.get(), 0);
                Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity);
            });
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledReadCallbackFailureBeforeEntriesAreReleasedKeepsPermitsUntilRelease()
            throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_callback_failure_before_release",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[] {1});
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) factory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            CompletableFuture<Void> readCompleted = new CompletableFuture<>();
            AtomicInteger failedCallbacks = new AtomicInteger();
            AtomicReference<List<Entry>> entriesReference = new AtomicReference<>();

            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entriesReference.set(entries);
                    readCompleted.complete(null);
                    throw new RuntimeException("Expected callback failure");
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    failedCallbacks.incrementAndGet();
                }
            }, new Object());

            readCompleted.join();
            Awaitility.await().pollDelay(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
                Assert.assertEquals(failedCallbacks.get(), 0);
                Assert.assertTrue(limiter.getRemainingBytes() < totalCapacity);
            });
            entriesReference.get().forEach(Entry::release);
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledReadFailureReleasesInflightReadsPermit() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_read_failure",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[] {1});
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) factory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            HoldingReadEntriesCallback callback = new HoldingReadEntriesCallback();

            ml.entryCache.asyncReadEntry(ml.currentLedger, 1, 1, () -> 0, callback, new Object());

            Awaitility.await().untilAsserted(() -> {
                Assert.assertTrue(callback.entries.isCompletedExceptionally());
                Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity);
            });
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledQueuedReadRunsAfterEntriesAreReleased() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_queued_read",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[9_000]);
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) factory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            HoldingReadEntriesCallback firstCallback = new HoldingReadEntriesCallback();
            HoldingReadEntriesCallback secondCallback = new HoldingReadEntriesCallback();

            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, firstCallback, new Object());
            List<Entry> firstEntries = firstCallback.entries.join();
            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, secondCallback, new Object());
            Assert.assertFalse(secondCallback.entries.isDone());

            firstEntries.forEach(Entry::release);
            List<Entry> secondEntries = secondCallback.entries.join();
            secondEntries.forEach(Entry::release);
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledReadLimitFailureIncludesDiagnosticContext() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        factoryConfig.setManagedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis(100);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_timeout",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[9_000]);
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) factory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            HoldingReadEntriesCallback firstCallback = new HoldingReadEntriesCallback();
            HoldingReadEntriesCallback secondCallback = new HoldingReadEntriesCallback();

            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, firstCallback, new Object());
            List<Entry> firstEntries = firstCallback.entries.join();
            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, secondCallback, new Object());
            Awaitility.await().untilAsserted(() ->
                    Assert.assertTrue(secondCallback.entries.isCompletedExceptionally()));

            Throwable exception = secondCallback.entries.handle((__, error) -> error).join();
            Assert.assertTrue(exception instanceof ManagedLedgerException.TooManyRequestsException);
            Assert.assertTrue(exception.getMessage().contains("ledger " + ml.currentLedger.getId()));
            Assert.assertTrue(exception.getMessage().contains(ml.getName()));
            Assert.assertTrue(exception.getMessage().contains("estimated read size"));
            Assert.assertTrue(exception.getMessage()
                    .contains("managedLedgerMaxReadsInFlightPermitsAcquireQueueSize"));
            Assert.assertTrue(exception.getMessage()
                    .contains("managedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis"));
            Assert.assertTrue(exception.getMessage().contains("managedLedgerMaxReadsInFlightSizeInMB"));

            firstEntries.forEach(Entry::release);
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledReadFailsImmediatelyWhenInflightReadsQueueIsFull() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        factoryConfig.setManagedLedgerMaxReadsInFlightPermitsAcquireQueueSize(0);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_queue_full",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[9_000]);
            InflightReadsLimiter limiter = ((RangeEntryCacheManagerImpl) factory.getEntryCacheManager())
                    .getInflightReadsLimiter();
            long totalCapacity = limiter.getRemainingBytes();
            HoldingReadEntriesCallback firstCallback = new HoldingReadEntriesCallback();
            HoldingReadEntriesCallback secondCallback = new HoldingReadEntriesCallback();

            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, firstCallback, new Object());
            List<Entry> firstEntries = firstCallback.entries.join();
            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, secondCallback, new Object());

            Throwable exception = secondCallback.entries.handle((__, error) -> error).join();
            Assert.assertTrue(exception instanceof ManagedLedgerException.TooManyRequestsException);
            Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity - 9_000
                    - RangeEntryCacheImpl.BOOKKEEPER_READ_OVERHEAD_PER_ENTRY);
            firstEntries.forEach(Entry::release);
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(limiter.getRemainingBytes(), totalCapacity));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledSingleEntryReadDoesNotInvalidateHandleOnLimiterRejection() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        factoryConfig.setManagedLedgerMaxReadsInFlightPermitsAcquireQueueSize(0);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_single_entry_rejection",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[9_000]);
            HoldingReadEntriesCallback holdingCallback = new HoldingReadEntriesCallback();
            ml.entryCache.asyncReadEntry(ml.currentLedger, 0, 0, () -> 0, holdingCallback, new Object());
            List<Entry> heldEntries = holdingCallback.entries.join();
            ReadHandle readHandle = Mockito.mock(ReadHandle.class);
            long ledgerId = ml.currentLedger.getId() + 1;
            Mockito.when(readHandle.getId()).thenReturn(ledgerId);
            Mockito.when(readHandle.getLength()).thenReturn(9_000L);
            Mockito.when(readHandle.getLastAddConfirmed()).thenReturn(0L);
            CompletableFuture<ManagedLedgerException> failure = new CompletableFuture<>();

            ml.entryCache.asyncReadEntry(readHandle, PositionFactory.create(ledgerId, 0),
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            failure.completeExceptionally(new AssertionError("Read should be rejected"));
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            failure.complete(exception);
                        }
                    }, new Object());

            Assert.assertTrue(failure.join() instanceof ManagedLedgerException.TooManyRequestsException);
            Mockito.verify(readHandle, Mockito.never()).closeAsync();
            heldEntries.forEach(Entry::release);
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledSingleEntryReadCompletesOnce() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_single_entry_success",
                    new ManagedLedgerConfig());
            ml.addEntry(new byte[] {1});
            CompletableFuture<Entry> completedEntry = new CompletableFuture<>();
            AtomicInteger failedCallbacks = new AtomicInteger();

            ml.entryCache.asyncReadEntry(ml.currentLedger,
                    PositionFactory.create(ml.currentLedger.getId(), 0), new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            completedEntry.complete(entry);
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            failedCallbacks.incrementAndGet();
                        }
                    }, new Object());

            completedEntry.join().release();
            Awaitility.await().pollDelay(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> Assert.assertEquals(failedCallbacks.get(), 0));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    public void testCacheDisabledSingleEntryReadInvalidatesHandleOnReadFailure() throws Exception {
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(10_000);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        try {
            ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("cache_disabled_limiter_single_entry_failure",
                    new ManagedLedgerConfig());
            ReadHandle readHandle = Mockito.mock(ReadHandle.class);
            long ledgerId = ml.currentLedger.getId() + 1;
            Mockito.when(readHandle.getId()).thenReturn(ledgerId);
            Mockito.when(readHandle.readAsync(0, 0)).thenReturn(CompletableFuture.failedFuture(
                    new ManagedLedgerException("Expected read failure")));
            Mockito.when(readHandle.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            CompletableFuture<ManagedLedgerException> failure = new CompletableFuture<>();

            ml.entryCache.asyncReadEntry(readHandle, PositionFactory.create(ledgerId, 0),
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            failure.completeExceptionally(new AssertionError("Read should fail"));
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            failure.complete(exception);
                        }
                    }, new Object());

            Assert.assertEquals(failure.join().getMessage(), "Expected read failure");
            Mockito.verify(readHandle).closeAsync();
        } finally {
            factory.shutdown();
        }
    }

    @DataProvider
    public Object[][] readMissingCases() {
        return new Object[][]{
                {"missRight"},
                {"missLeft"},
                {"bothMiss"}
        };
    }

    @Test(dataProvider = "readMissingCases")
    public void testPreciseLimitation(String missingCase) throws Exception {
        final long start1 = 50;
        final long start2 = "missLeft".endsWith(missingCase) || "bothMiss".equals(missingCase) ? 30 : 50;
        final long end1 = 99;
        final long end2 = "missRight".endsWith(missingCase) || "bothMiss".equals(missingCase) ? 109 : 99;
        final HashSet<Long> secondReadEntries = new HashSet<>();
        if (start2 < start1) {
            secondReadEntries.add(start2);
        }
        if (end2 > end1) {
            secondReadEntries.add(end1 + 1);
        }
        final int readCount1 = (int) (end1 - start1 + 1);
        final int readCount2 = (int) (end2 - start2 + 1);

        final DefaultThreadFactory threadFactory = new DefaultThreadFactory(UUID.randomUUID().toString());
        final ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(100000);
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setCacheEvictionIntervalMs(3600 * 1000);
        factoryConfig.setManagedLedgerMaxReadsInFlightSize(1000_000);
        final ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        final ManagedLedgerImpl ml = (ManagedLedgerImpl) factory.open("my_test_ledger", config);
        final RangeEntryCacheImpl entryCache = (RangeEntryCacheImpl) ml.entryCache;
        final RangeEntryCacheManagerImpl rangeEntryCacheManager =
                (RangeEntryCacheManagerImpl) factory.getEntryCacheManager();
        final InflightReadsLimiter limiter = rangeEntryCacheManager.getInflightReadsLimiter();
        final long totalCapacity = limiter.getRemainingBytes();
        // final ManagedCursorImpl c1 = (ManagedCursorImpl) ml.openCursor("c1");
        for (byte i = 1; i < 127; i++) {
            log.info().attr("entryIndex", i).log("Add entry");
            ml.addEntry(new byte[]{i});
        }
        // Evict cached entries.
        entryCache.clear();
        Assert.assertEquals(entryCache.getSize(), 0);

        CountDownLatch readCompleteSignal1 = new CountDownLatch(1);
        CountDownLatch readCompleteSignal2 = new CountDownLatch(1);
        CountDownLatch firstReadingStarted = new CountDownLatch(1);
        LedgerHandle currentLedger = ml.currentLedger;
        LedgerHandle spyCurrentLedger = Mockito.spy(currentLedger);
        ml.currentLedger = spyCurrentLedger;
        Answer<?> answer = invocation -> {
            long firstEntry = (long) invocation.getArguments()[0];
            log.info().attr("firstEntry", firstEntry).log("Reading entry");
            if (firstEntry == start1) {
                // Wait 3s to make
                firstReadingStarted.countDown();
                readCompleteSignal1.await();
                Object res = invocation.callRealMethod();
                return res;
            } else if (secondReadEntries.contains(firstEntry)) {
                final CompletableFuture<Object> res = new CompletableFuture<>();
                threadFactory.newThread(() -> {
                    try {
                        readCompleteSignal2.await();
                        @SuppressWarnings("unchecked")
                        CompletableFuture<LedgerEntries> future =
                                (CompletableFuture<LedgerEntries>) invocation.callRealMethod();
                        future.thenAccept(v -> {
                            res.complete(v);
                        }).exceptionally(ex -> {
                            res.completeExceptionally(ex);
                            return null;
                        });
                    } catch (Throwable ex) {
                        res.completeExceptionally(ex);
                    }
                }).start();
                return res;
            } else {
                return invocation.callRealMethod();
            }
        };
        doAnswer(answer).when(spyCurrentLedger).readAsync(anyLong(), anyLong());
        doAnswer(answer).when(spyCurrentLedger).readUnconfirmedAsync(anyLong(), anyLong());

        // Initialize "entryCache.estimatedEntrySize" to the correct value.
        Object ctx = new Object();
        SimpleReadEntriesCallback cb0 = new SimpleReadEntriesCallback();
        entryCache.asyncReadEntry(spyCurrentLedger, 125, 125, () -> 1, cb0, ctx);
        cb0.entries.join();
        int sizePerEntry = Long.valueOf(entryCache.getEstimatedEntrySize(ml.currentLedger)).intValue();
        Awaitility.await().untilAsserted(() -> {
            long remainingBytes = limiter.getRemainingBytes();
            Assert.assertEquals(remainingBytes, totalCapacity);
        });
        log.info().attr("remainingBytes", limiter.getRemainingBytes()).log("Remaining bytes after init");

        // Concurrency reading.

        SimpleReadEntriesCallback cb1 = new SimpleReadEntriesCallback();
        SimpleReadEntriesCallback cb2 = new SimpleReadEntriesCallback();
        threadFactory.newThread(() -> {
            entryCache.asyncReadEntry(spyCurrentLedger, start1, end1, () -> 1, cb1, ctx);
        }).start();
        threadFactory.newThread(() -> {
            try {
                firstReadingStarted.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            entryCache.asyncReadEntry(spyCurrentLedger, start2, end2, () -> 1, cb2, ctx);
        }).start();

        long bytesAcquired1 = calculateBytesSizeBeforeFirstReading(readCount1 + readCount2, sizePerEntry);
        long remainingBytesExpected1 = totalCapacity - bytesAcquired1;
        log.info().attr("bytesAcquired", bytesAcquired1).log("Acquired bytes before first reading");
        log.info().attr("remainingBytesExpected", remainingBytesExpected1)
                .log("Remaining bytes expected before first reading");
        Awaitility.await().untilAsserted(() -> {
            log.info().attr("remainingBytes", limiter.getRemainingBytes())
                    .log("Remaining bytes before first reading completes");
            Assert.assertEquals(limiter.getRemainingBytes(), remainingBytesExpected1);
        });

        // Complete the read1.
        Thread.sleep(3000);
        readCompleteSignal1.countDown();
        cb1.entries.join();
        long bytesAcquired2 = calculateBytesSizeBeforeFirstReading(readCount2, sizePerEntry);
        long remainingBytesExpected2 = totalCapacity - bytesAcquired2;
        log.info().attr("bytesAcquired", bytesAcquired2).log("Acquired bytes after first reading");
        log.info().attr("remainingBytesExpected", remainingBytesExpected2)
                .log("Remaining bytes expected after first reading");
        Awaitility.await().untilAsserted(() -> {
            log.info().attr("remainingBytes", limiter.getRemainingBytes())
                    .log("Remaining bytes after first reading completes");
            Assert.assertEquals(limiter.getRemainingBytes(), remainingBytesExpected2);
        });

        readCompleteSignal2.countDown();
        cb2.entries.join();
        Awaitility.await().untilAsserted(() -> {
            long remainingBytes = limiter.getRemainingBytes();
            log.info().attr("remainingBytes", remainingBytes).log("Remaining bytes after all readings complete");
            Assert.assertEquals(remainingBytes, totalCapacity);
        });
        // cleanup
        ml.delete();
        factory.shutdown();
    }

    private long calculateBytesSizeBeforeFirstReading(int entriesCount, int perEntrySize) {
        return entriesCount * perEntrySize;
    }

    class SimpleReadEntriesCallback implements AsyncCallbacks.ReadEntriesCallback {

        CompletableFuture<List<Byte>> entries = new CompletableFuture<>();

        @Override
        public void readEntriesComplete(List<Entry> entriesRead, Object ctx) {
            List<Byte> list = new ArrayList<>(entriesRead.size());
            for (Entry entry : entriesRead) {
                byte b = entry.getDataBuffer().readByte();
                list.add(b);
                entry.release();
            }
            this.entries.complete(list);
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            this.entries.completeExceptionally(exception);
        }
    }

    class HoldingReadEntriesCallback implements AsyncCallbacks.ReadEntriesCallback {

        CompletableFuture<List<Entry>> entries = new CompletableFuture<>();

        @Override
        public void readEntriesComplete(List<Entry> entriesRead, Object ctx) {
            entries.complete(entriesRead);
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            entries.completeExceptionally(exception);
        }
    }
}

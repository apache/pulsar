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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.rawEntryConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReadCompletionAffinityTest extends MockedBookKeeperTestCase {
    @DataProvider
    public Object[][] inlineCompletion() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "inlineCompletion")
    public void testCacheHitCompletionAffinity(boolean inline) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-affinity-" + inline,
                rawEntryConfig());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            ledger.addEntry(new byte[] {1});
            assertTrue(ledger.entryCache.getSize() > 0, "The test requires a cache hit");
            Thread worker = blockWorker(ledger, releaseWorker);
            Thread caller = Thread.currentThread();
            CompletableFuture<Thread> completed = new CompletableFuture<>();
            cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public boolean canExecuteOnAnyThread() {
                    return inline;
                }

                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entries.forEach(Entry::release);
                    completed.complete(Thread.currentThread());
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    completed.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);
            // A blocked worker makes the queue boundary deterministic, rather than depending on task timing.
            assertEquals(completed.isDone(), inline);
            releaseWorker.countDown();
            assertSame(completed.get(10, TimeUnit.SECONDS), inline ? caller : worker);
        } finally {
            releaseWorker.countDown();
            ledger.close();
        }
    }

    @Test
    public void testInlineFutureKeepsDispatcherBoundary() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-dispatcher-boundary",
                rawEntryConfig());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ExecutorService dispatcher = Executors.newSingleThreadExecutor();
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            ledger.addEntry(new byte[] {1});
            assertTrue(ledger.entryCache.getSize() > 0, "The test requires a cache hit");
            Thread worker = blockWorker(ledger, releaseWorker);
            Thread caller = Thread.currentThread();
            CompletableFuture<List<Entry>> read = ManagedLedgerUtils.readEntriesWithSkipOrWait(
                    cursor, 1, Long.MAX_VALUE, PositionFactory.LATEST, null, true);
            CompletableFuture<Thread> continued = read.thenApplyAsync(entries -> {
                entries.forEach(Entry::release);
                return Thread.currentThread();
            }, dispatcher);
            assertTrue(read.isDone(), "A cache hit must not wait behind unrelated ledger work");
            Thread continuationThread = continued.get(10, TimeUnit.SECONDS);
            assertNotSame(continuationThread, caller);
            assertNotSame(continuationThread, worker);
        } finally {
            releaseWorker.countDown();
            ledger.close();
            dispatcher.shutdownNow();
            assertTrue(dispatcher.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static Thread blockWorker(ManagedLedgerImpl ledger, CountDownLatch releaseWorker) throws Exception {
        CompletableFuture<Thread> started = new CompletableFuture<>();
        ledger.getExecutor().execute(() -> {
            started.complete(Thread.currentThread());
            try {
                releaseWorker.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return started.get(10, TimeUnit.SECONDS);
    }

    @Test(dataProvider = "inlineCompletion")
    public void testCacheMissAcrossLedgers(boolean inline) throws Exception {
        ManagedLedgerConfig config = rawEntryConfig().setMaxEntriesPerLedger(2);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-rollover-" + inline, config);
        AtomicInteger storageReads = new AtomicInteger();
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            for (int i = 0; i < 6; i++) {
                ledger.addEntry(new byte[] {(byte) i});
            }
            ledger.entryCache.clear();
            bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
                storageReads.incrementAndGet();
                return CompletableFuture.completedFuture(entries);
            });
            List<Entry> entries = ManagedLedgerUtils.readEntriesWithSkipOrWait(
                    cursor, 6, Long.MAX_VALUE, PositionFactory.LATEST, null, inline).get(10, TimeUnit.SECONDS);
            try {
                assertThat(entries).hasSize(6);
                for (int i = 0; i < entries.size(); i++) {
                    assertThat(entries.get(i).getData()).containsExactly((byte) i);
                }
                assertThat(entries.stream().map(entry -> entry.getPosition().getLedgerId()).distinct().count())
                        .isEqualTo(3);
                assertThat(storageReads.get()).isGreaterThanOrEqualTo(3);
            } finally {
                entries.forEach(Entry::release);
            }
        } finally {
            bkc.setReadHandleInterceptor(null);
            ledger.close();
        }
    }

    @Test(dataProvider = "inlineCompletion")
    public void testStorageFailureAndRetry(boolean inline) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-failure-" + inline,
                rawEntryConfig());
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            ledger.addEntry(new byte[] {1});
            ledger.entryCache.clear();
            bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
                entries.close();
                return CompletableFuture.failedFuture(BKException.create(BKException.Code.ReadException));
            });
            var failed = ManagedLedgerUtils.readEntriesWithSkipOrWait(
                    cursor, 1, Long.MAX_VALUE, PositionFactory.LATEST, null, inline);
            assertThatThrownBy(() -> failed.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            bkc.setReadHandleInterceptor(null);
            List<Entry> entries = ManagedLedgerUtils.readEntriesWithSkipOrWait(
                    cursor, 1, Long.MAX_VALUE, PositionFactory.LATEST, null, inline).get(10, TimeUnit.SECONDS);
            try {
                assertThat(entries).hasSize(1);
                assertThat(entries.get(0).getData()).containsExactly((byte) 1);
            } finally {
                entries.forEach(Entry::release);
            }
        } finally {
            bkc.setReadHandleInterceptor(null);
            ledger.close();
        }
    }
}

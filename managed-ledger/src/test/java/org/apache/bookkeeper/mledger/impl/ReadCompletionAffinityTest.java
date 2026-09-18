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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ScanOutcome;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReadCompletionAffinityTest extends MockedBookKeeperTestCase {
    @Test
    public void testIndependentCacheHitsDoNotAccumulateCompletionDepth() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-affinity-", inlineConfig());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            int count = OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS * 3;
            for (int i = 0; i < count; i++) {
                ledger.addEntry(new byte[] {1});
            }
            assertTrue(ledger.entryCache.getSize() > 0, "The test requires a cache hit");
            blockWorker(ledger, releaseWorker);
            Thread caller = Thread.currentThread();
            // Each independent completion must unwind its depth, allowing subsequent reads to stay inline
            // even after more reads than the nesting limit. A blocked worker makes unexpected queuing deterministic.
            for (int i = 0; i < count; i++) {
                CompletableFuture<Thread> completed = new CompletableFuture<>();
                cursor.asyncReadEntries(1, new ReadEntriesCallback() {
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
                assertTrue(completed.isDone(), "Independent read " + i + " must complete inline");
                assertSame(completed.get(10, TimeUnit.SECONDS), caller);
            }
        } finally {
            releaseWorker.countDown();
            ledger.close();
        }
    }

    @Test
    public void testInlineFutureKeepsDispatcherBoundary() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-dispatcher-boundary",
                inlineConfig());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ExecutorService dispatcher = Executors.newSingleThreadExecutor();
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            ledger.addEntry(new byte[] {1});
            assertTrue(ledger.entryCache.getSize() > 0, "The test requires a cache hit");
            Thread worker = blockWorker(ledger, releaseWorker);
            Thread caller = Thread.currentThread();
            CompletableFuture<List<Entry>> read = ManagedLedgerUtils.readEntriesWithSkipOrWait(
                    cursor, 1, Long.MAX_VALUE, PositionFactory.LATEST, null);
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

    @Test(timeOut = 60000)
    public void testCachedScanFromCallingThreadBoundsRecursion() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-recursion", inlineConfig());
        try {
            ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
            int count = 1000;
            for (int i = 0; i < count; i++) {
                ledger.addEntry(new byte[] {1});
            }
            AtomicInteger seen = new AtomicInteger();
            AtomicInteger firstDepth = new AtomicInteger(-1);
            AtomicInteger maxDepth = new AtomicInteger();
            var outcome = cursor.scan(Optional.empty(), entry -> {
                int depth = Thread.currentThread().getStackTrace().length;
                firstDepth.compareAndSet(-1, depth);
                maxDepth.accumulateAndGet(depth, Math::max);
                seen.incrementAndGet();
                return true;
            }, 1, Long.MAX_VALUE, Long.MAX_VALUE);
            assertThat(outcome.get(30, TimeUnit.SECONDS)).isEqualTo(ScanOutcome.COMPLETED);
            assertThat(seen.get()).isEqualTo(count);
            assertThat(maxDepth.get() - firstDepth.get())
                    .isLessThan(OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS * 40);
        } finally {
            ledger.close();
        }
    }

    @Test(timeOut = 30000)
    public void testNestedCacheHitsHopAtDepthLimitAndIndependentReadOvertakes() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-depth-limit", inlineConfig());
        CountDownLatch releaseQueuedCompletion = new CountDownLatch(1);
        try {
            ManagedCursor nestedCursor = ledger.openCursor("nested");
            ManagedCursor independentCursor = ledger.openCursor("independent");
            int count = OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 1;
            List<Position> positions = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                positions.add(ledger.addEntry(new byte[] {(byte) i}));
            }
            ledger.entryCache.clear();
            List<ByteBuf> cachedData = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                cachedData.add(cacheEntry(ledger, positions.get(i), (byte) i));
            }
            assertThat(ledger.entryCache.getSize()).isPositive();

            CompletableFuture<Thread> ledgerWorker = new CompletableFuture<>();
            ledger.getExecutor().execute(() -> ledgerWorker.complete(Thread.currentThread()));
            Thread worker = ledgerWorker.get(10, TimeUnit.SECONDS);
            Thread caller = Thread.currentThread();
            assertThat(caller).isNotSameAs(worker);

            AtomicInteger nestedCompletions = new AtomicInteger();
            AtomicInteger laterCompletionOrder = new AtomicInteger();
            AtomicInteger independentOrder = new AtomicInteger();
            AtomicInteger queuedOrder = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            AtomicReference<Thread> unexpectedInlineThread = new AtomicReference<>();
            AtomicReference<ForkJoinPool> queuedPool = new AtomicReference<>();
            CompletableFuture<Thread> queuedCompletionStarted = new CompletableFuture<>();
            CompletableFuture<Thread> queuedCompletion = new CompletableFuture<>();
            nestedCursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entries.forEach(Entry::release);
                    int completion = nestedCompletions.incrementAndGet();
                    if (completion < count) {
                        if (Thread.currentThread() != caller) {
                            unexpectedInlineThread.compareAndSet(null, Thread.currentThread());
                        }
                        nestedCursor.asyncReadEntries(1, this, null, PositionFactory.LATEST);
                    } else {
                        queuedPool.set(ForkJoinTask.getPool());
                        queuedCompletionStarted.complete(Thread.currentThread());
                        try {
                            if (!awaitQueuedCompletion(releaseQueuedCompletion)) {
                                queuedCompletion.completeExceptionally(
                                        new IllegalStateException("Timed out waiting to release queued completion"));
                                return;
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            queuedCompletion.completeExceptionally(e);
                            return;
                        }
                        queuedOrder.set(laterCompletionOrder.incrementAndGet());
                        queuedCompletion.complete(Thread.currentThread());
                    }
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    failures.incrementAndGet();
                    queuedCompletionStarted.completeExceptionally(exception);
                    queuedCompletion.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);

            Thread queuedThread = queuedCompletionStarted.get(10, TimeUnit.SECONDS);
            assertThat(nestedCompletions.get()).isEqualTo(count);
            assertThat(unexpectedInlineThread.get()).isNull();
            assertThat(queuedThread).isNotSameAs(caller);
            if (ForkJoinPool.getCommonPoolParallelism() > 1) {
                assertThat(queuedPool.get()).isSameAs(ForkJoinPool.commonPool());
                assertThat(queuedThread).isNotSameAs(worker);
            } else {
                assertThat(queuedThread).isSameAs(worker);
            }
            assertThat(queuedCompletion).isNotDone();

            // A separate cursor makes this completion independent. Overlapping active reads on one cursor
            // are unsupported.
            CompletableFuture<Thread> independentCompletion = new CompletableFuture<>();
            independentCursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entries.forEach(Entry::release);
                    independentOrder.set(laterCompletionOrder.incrementAndGet());
                    independentCompletion.complete(Thread.currentThread());
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    independentCompletion.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);
            assertThat(independentCompletion).isDone();
            assertThat(independentCompletion.get(10, TimeUnit.SECONDS)).isSameAs(caller);
            assertThat(independentOrder.get()).isEqualTo(1);
            assertThat(queuedCompletion).isNotDone();

            releaseQueuedCompletion.countDown();
            assertThat(queuedCompletion.get(10, TimeUnit.SECONDS)).isSameAs(queuedThread);
            assertThat(queuedOrder.get()).isEqualTo(2);
            assertThat(nestedCompletions.get()).isEqualTo(count);
            assertThat(failures.get()).isZero();
            for (ByteBuf data : cachedData) {
                assertThat(data.refCnt()).isEqualTo(1); // Only the cache retains the buffer after completion.
            }
        } finally {
            releaseQueuedCompletion.countDown();
            ledger.close();
        }
    }

    @Test(timeOut = 30000)
    public void testNestedCachedChainReturnsToLedgerExecutorAcrossLedgers() throws Exception {
        int firstLedgerCount = OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 2;
        ManagedLedgerConfig config = inlineConfig().setMaxEntriesPerLedger(firstLedgerCount);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-cached-chain-rollover", config);
        AtomicInteger storageReads = new AtomicInteger();
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            List<Position> positions = new ArrayList<>();
            for (int i = 0; i <= firstLedgerCount; i++) {
                positions.add(ledger.addEntry(new byte[] {(byte) i}));
            }
            assertThat(positions.get(firstLedgerCount).getLedgerId())
                    .isNotEqualTo(positions.get(0).getLedgerId());
            // Resolve the closed ledger handle before starting the chain so opening it cannot cause a handoff.
            ledger.getLedgerHandle(positions.get(0).getLedgerId()).get(10, TimeUnit.SECONDS);
            ledger.entryCache.clear();
            for (int i = 0; i < positions.size(); i++) {
                cacheEntry(ledger, positions.get(i), (byte) i);
            }
            bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
                storageReads.incrementAndGet();
                return CompletableFuture.completedFuture(entries);
            });
            CompletableFuture<Thread> ledgerWorker = new CompletableFuture<>();
            ledger.getExecutor().execute(() -> ledgerWorker.complete(Thread.currentThread()));
            Thread worker = ledgerWorker.get(10, TimeUnit.SECONDS);
            Thread caller = Thread.currentThread();

            AtomicInteger completions = new AtomicInteger();
            AtomicReference<Thread> overflowThread = new AtomicReference<>();
            AtomicReference<ForkJoinPool> overflowPool = new AtomicReference<>();
            AtomicReference<Thread> boundaryThread = new AtomicReference<>();
            AtomicReference<ForkJoinPool> boundaryPool = new AtomicReference<>();
            CompletableFuture<List<Position>> crossedEntries = new CompletableFuture<>();
            cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    List<Position> returnedPositions = entries.stream().map(Entry::getPosition).toList();
                    entries.forEach(Entry::release);
                    int completion = completions.incrementAndGet();
                    if (completion <= OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS) {
                        cursor.asyncReadEntries(1, this, null, PositionFactory.LATEST);
                    } else if (completion == OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 1) {
                        overflowThread.set(Thread.currentThread());
                        overflowPool.set(ForkJoinTask.getPool());
                        // One entry remains in the first ledger. Reading two forces checkReadCompletion to
                        // schedule the continuation on the ledger executor, even though both entries are cached.
                        cursor.asyncReadEntries(2, this, null, PositionFactory.LATEST);
                    } else {
                        boundaryThread.set(Thread.currentThread());
                        boundaryPool.set(ForkJoinTask.getPool());
                        crossedEntries.complete(returnedPositions);
                    }
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    crossedEntries.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);

            assertThat(crossedEntries.get(10, TimeUnit.SECONDS))
                    .containsExactly(positions.get(firstLedgerCount - 1), positions.get(firstLedgerCount));
            assertThat(overflowThread.get()).isNotSameAs(caller);
            if (ForkJoinPool.getCommonPoolParallelism() > 1) {
                assertThat(overflowPool.get()).isSameAs(ForkJoinPool.commonPool());
                assertThat(overflowThread.get()).isNotSameAs(worker);
            } else {
                assertThat(overflowThread.get()).isSameAs(worker);
            }
            assertThat(boundaryThread.get()).isSameAs(worker);
            assertThat(boundaryPool.get()).isNull();
            assertThat(completions.get()).isEqualTo(OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 2);
            assertThat(storageReads.get()).isZero();
        } finally {
            bkc.setReadHandleInterceptor(null);
            ledger.close();
        }
    }

    @Test
    public void testLegacyCachedReadQueuesCompletionOffLedgerAndRunsItInlineOnLedger() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-default-legacy", rawEntryConfig());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            ledger.addEntry(new byte[] {1});
            ledger.addEntry(new byte[] {2});
            assertThat(ledger.entryCache.getSize()).isPositive();

            Thread worker = blockWorker(ledger, releaseWorker);
            CompletableFuture<Thread> fromCaller = readOne(cursor, false);
            assertThat(fromCaller).isNotDone();
            releaseWorker.countDown();
            assertThat(fromCaller.get(10, TimeUnit.SECONDS)).isSameAs(worker);

            CompletableFuture<Boolean> completedInlineOnWorker = new CompletableFuture<>();
            CompletableFuture<Thread> fromWorker = new CompletableFuture<>();
            ledger.getExecutor().execute(() -> {
                cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        entries.forEach(Entry::release);
                        fromWorker.complete(Thread.currentThread());
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        fromWorker.completeExceptionally(exception);
                    }
                }, null, PositionFactory.LATEST);
                completedInlineOnWorker.complete(fromWorker.isDone());
            });
            assertThat(completedInlineOnWorker.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(fromWorker.get(10, TimeUnit.SECONDS)).isSameAs(worker);
        } finally {
            releaseWorker.countDown();
            ledger.close();
        }
    }

    @Test(timeOut = 30000)
    public void testLegacyCachedReadOnLedgerWorkerQueuesAtDepthLimit() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-legacy-depth-limit", rawEntryConfig());
        CountDownLatch releaseOuterRead = new CountDownLatch(1);
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            int count = OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 1;
            for (int i = 0; i < count; i++) {
                ledger.addEntry(new byte[] {(byte) i});
            }
            assertThat(ledger.entryCache.getSize()).isPositive();

            AtomicInteger completions = new AtomicInteger();
            AtomicReference<Thread> worker = new AtomicReference<>();
            AtomicReference<Thread> unexpectedThread = new AtomicReference<>();
            CompletableFuture<Integer> inlineCompletions = new CompletableFuture<>();
            CompletableFuture<Thread> queuedCompletion = new CompletableFuture<>();
            ledger.getExecutor().execute(() -> {
                worker.set(Thread.currentThread());
                cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        entries.forEach(Entry::release);
                        if (Thread.currentThread() != worker.get()) {
                            unexpectedThread.compareAndSet(null, Thread.currentThread());
                        }
                        int completion = completions.incrementAndGet();
                        if (completion < count) {
                            cursor.asyncReadEntries(1, this, null, PositionFactory.LATEST);
                        } else {
                            queuedCompletion.complete(Thread.currentThread());
                        }
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        queuedCompletion.completeExceptionally(exception);
                    }
                }, null, PositionFactory.LATEST);
                inlineCompletions.complete(completions.get());
                try {
                    releaseOuterRead.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    queuedCompletion.completeExceptionally(e);
                }
            });

            assertThat(inlineCompletions.get(10, TimeUnit.SECONDS))
                    .isEqualTo(OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS);
            assertThat(unexpectedThread.get()).isNull();
            assertThat(queuedCompletion).isNotDone();

            releaseOuterRead.countDown();
            assertThat(queuedCompletion.get(10, TimeUnit.SECONDS)).isSameAs(worker.get());
        } finally {
            releaseOuterRead.countDown();
            ledger.close();
        }
    }

    @Test
    public void testRejectedLedgerExecutorReleasesEntriesAndCompletesFailureOnce() throws Exception {
        ManagedLedgerImpl ledger = spy((ManagedLedgerImpl) factory.open("completion-executor-rejection",
                rawEntryConfig()));
        try {
            ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
            Position first = ledger.addEntry(new byte[] {1});
            Position second = ledger.addEntry(new byte[] {2});
            ledger.entryCache.clear();
            ByteBuf firstData = cacheEntry(ledger, first, (byte) 1);
            ThreadBoundExecutor rejectingExecutor = mock(ThreadBoundExecutor.class);
            doReturn(false).when(rejectingExecutor).isCurrentThread();
            doThrow(new RejectedExecutionException("test ledger-executor rejection"))
                    .when(rejectingExecutor).execute(any(Runnable.class));
            doReturn(rejectingExecutor).when(ledger).getExecutor();

            AtomicInteger completed = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    completed.incrementAndGet();
                    entries.forEach(Entry::release);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    failures.incrementAndGet();
                }
            }, null, PositionFactory.LATEST);

            assertThat(completed.get()).isZero();
            assertThat(failures.get()).isEqualTo(1);
            assertThat(cursor.getPendingReadOpsCount()).isZero();
            assertThat(firstData.refCnt()).isEqualTo(1); // Only the cache owns the rejected entry's buffer.
            assertThat(cursor.getReadPosition()).isEqualTo(second);
            verify(rejectingExecutor, times(1)).execute(any(Runnable.class));
        } finally {
            ledger.close();
        }
    }

    @DataProvider
    public Object[][] rejectedDepthLimitCompletionModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "rejectedDepthLimitCompletionModes")
    public void testRejectedLedgerExecutorAfterDepthLimitReleasesEntriesAndCompletesFailureOnce(boolean inline)
            throws Exception {
        if (inline && ForkJoinPool.getCommonPoolParallelism() > 1) {
            throw new SkipException("Inline ledger-executor fallback requires a fresh test JVM with "
                    + "-Djava.util.concurrent.ForkJoinPool.common.parallelism=1");
        }
        ManagedLedgerImpl ledger = spy((ManagedLedgerImpl) factory.open("completion-depth-rejection-" + inline,
                rawEntryConfig().setReadEntriesCallbackInline(inline)));
        try {
            ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
            int count = OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 1;
            List<Position> positions = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                positions.add(ledger.addEntry(new byte[] {(byte) i}));
            }
            ledger.entryCache.clear();
            ByteBuf rejectedData = null;
            for (int i = 0; i < count; i++) {
                ByteBuf data = cacheEntry(ledger, positions.get(i), (byte) i);
                if (i == count - 1) {
                    rejectedData = data;
                }
            }
            Position rejectedPosition = positions.get(count - 1);
            assertThat(rejectedData).isNotNull();
            ThreadBoundExecutor rejectingExecutor = mock(ThreadBoundExecutor.class);
            doReturn(true).when(rejectingExecutor).isCurrentThread();
            doThrow(new RejectedExecutionException("test depth-limit handoff rejection"))
                    .when(rejectingExecutor).execute(any(Runnable.class));
            doReturn(rejectingExecutor).when(ledger).getExecutor();

            AtomicInteger completed = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            AtomicReference<ManagedLedgerException> failure = new AtomicReference<>();
            cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    completed.incrementAndGet();
                    entries.forEach(Entry::release);
                    cursor.asyncReadEntries(1, this, null, PositionFactory.LATEST);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    failures.incrementAndGet();
                    failure.set(exception);
                }
            }, null, PositionFactory.LATEST);

            assertThat(failure.get()).isNotNull();
            assertThat(failures.get()).isEqualTo(1);
            assertThat(completed.get()).isEqualTo(OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS);
            assertThat(cursor.getPendingReadOpsCount()).isZero();
            assertThat(rejectedData.refCnt()).isEqualTo(1); // Only the cache owns the rejected entry's buffer.
            assertThat(cursor.getReadPosition()).isEqualTo(rejectedPosition.getNext());
            verify(rejectingExecutor, times(1)).execute(any(Runnable.class));
        } finally {
            ledger.close();
        }
    }

    @Test
    public void testReadCompletionPolicyIsCapturedWhenLedgerOpens() throws Exception {
        for (boolean inlineAtOpen : List.of(false, true)) {
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-captured-policy-" + inlineAtOpen,
                    rawEntryConfig().setReadEntriesCallbackInline(inlineAtOpen));
            CountDownLatch releaseWorker = new CountDownLatch(1);
            try {
                ManagedCursor cursor = ledger.openCursor("cursor");
                ledger.addEntry(new byte[] {1});
                assertThat(ledger.isReadEntriesCallbackInline()).isEqualTo(inlineAtOpen);
                ledger.getConfig().setReadEntriesCallbackInline(!inlineAtOpen);
                assertThat(ledger.isReadEntriesCallbackInline()).isEqualTo(inlineAtOpen);
                ledger.setConfig(rawEntryConfig().setReadEntriesCallbackInline(!inlineAtOpen));
                assertThat(ledger.isReadEntriesCallbackInline()).isEqualTo(inlineAtOpen);

                Thread worker = blockWorker(ledger, releaseWorker);
                Thread caller = Thread.currentThread();
                CompletableFuture<Thread> callback = readOne(cursor, false);
                assertThat(callback.isDone()).isEqualTo(inlineAtOpen);
                if (inlineAtOpen) {
                    assertThat(callback.get(10, TimeUnit.SECONDS)).isSameAs(caller);
                } else {
                    releaseWorker.countDown();
                    assertThat(callback.get(10, TimeUnit.SECONDS)).isSameAs(worker);
                }
            } finally {
                releaseWorker.countDown();
                ledger.close();
            }
        }
    }

    private static boolean awaitQueuedCompletion(CountDownLatch latch) throws InterruptedException {
        if (!ForkJoinTask.inForkJoinPool()) {
            return latch.await(20, TimeUnit.SECONDS);
        }
        ForkJoinPool.ManagedBlocker blocker = new ForkJoinPool.ManagedBlocker() {
            private final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);

            @Override
            public boolean block() throws InterruptedException {
                long remaining = deadline - System.nanoTime();
                if (remaining > 0) {
                    latch.await(remaining, TimeUnit.NANOSECONDS);
                }
                return true;
            }

            @Override
            public boolean isReleasable() {
                return latch.getCount() == 0 || System.nanoTime() >= deadline;
            }
        };
        ForkJoinPool.managedBlock(blocker);
        return latch.getCount() == 0;
    }

    private static ByteBuf cacheEntry(ManagedLedgerImpl ledger, Position position, byte value) {
        ByteBuf data = Unpooled.wrappedBuffer(new byte[] {value});
        EntryImpl entry = EntryImpl.create(position, data, 0);
        data.release();
        try {
            assertThat(ledger.entryCache.insert(entry)).isTrue();
            return data;
        } finally {
            entry.release();
        }
    }

    private static ManagedLedgerConfig inlineConfig() {
        return rawEntryConfig().setReadEntriesCallbackInline(true);
    }

    private static CompletableFuture<Thread> readOne(ManagedCursor cursor, boolean wait) {
        CompletableFuture<Thread> completed = new CompletableFuture<>();
        ReadEntriesCallback callback = new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                entries.forEach(Entry::release);
                completed.complete(Thread.currentThread());
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                completed.completeExceptionally(exception);
            }
        };
        if (wait) {
            cursor.asyncReadEntriesOrWait(1, callback, null, PositionFactory.LATEST);
        } else {
            cursor.asyncReadEntries(1, callback, null, PositionFactory.LATEST);
        }
        return completed;
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

    @Test
    public void testCacheMissAcrossLedgers() throws Exception {
        ManagedLedgerConfig config = inlineConfig().setMaxEntriesPerLedger(2);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-rollover-", config);
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
                    cursor, 6, Long.MAX_VALUE, PositionFactory.LATEST, null).get(10, TimeUnit.SECONDS);
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

    @Test
    public void testStorageFailureAndRetry() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-failure-",
                inlineConfig());
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            ledger.addEntry(new byte[] {1});
            ledger.entryCache.clear();
            bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
                entries.close();
                return CompletableFuture.failedFuture(BKException.create(BKException.Code.ReadException));
            });
            var failed = ManagedLedgerUtils.readEntriesWithSkipOrWait(
                    cursor, 1, Long.MAX_VALUE, PositionFactory.LATEST, null);
            assertThatThrownBy(() -> failed.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            bkc.setReadHandleInterceptor(null);
            List<Entry> entries = ManagedLedgerUtils.readEntriesWithSkipOrWait(
                    cursor, 1, Long.MAX_VALUE, PositionFactory.LATEST, null).get(10, TimeUnit.SECONDS);
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

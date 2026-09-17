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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BKException;
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
    public void testNestedCacheHitsQueueAtDepthLimitAndIndependentReadOvertakes() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-depth-limit", inlineConfig());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            ManagedCursor nestedCursor = ledger.openCursor("nested");
            ManagedCursor independentCursor = ledger.openCursor("independent");
            int count = OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 1;
            for (int i = 0; i < count; i++) {
                ledger.addEntry(new byte[] {(byte) i});
            }
            assertThat(ledger.entryCache.getSize()).isPositive();

            Thread worker = blockWorker(ledger, releaseWorker);
            Thread caller = Thread.currentThread();
            assertThat(caller).isNotSameAs(worker);

            AtomicInteger nestedCompletions = new AtomicInteger();
            AtomicInteger laterCompletionOrder = new AtomicInteger();
            AtomicInteger independentOrder = new AtomicInteger();
            AtomicInteger queuedOrder = new AtomicInteger();
            AtomicReference<Thread> unexpectedInlineThread = new AtomicReference<>();
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
                        queuedOrder.set(laterCompletionOrder.incrementAndGet());
                        queuedCompletion.complete(Thread.currentThread());
                    }
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    queuedCompletion.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);

            assertThat(nestedCompletions.get()).isEqualTo(OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS);
            assertThat(unexpectedInlineThread.get()).isNull();
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

            releaseWorker.countDown();
            assertThat(queuedCompletion.get(10, TimeUnit.SECONDS)).isSameAs(worker);
            assertThat(queuedOrder.get()).isEqualTo(2);
        } finally {
            releaseWorker.countDown();
            ledger.close();
        }
    }

    @Test
    public void testDefaultCachedReadQueuesCompletionFromCallingAndLedgerThreads() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-default-legacy", rawEntryConfig());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            ledger.addEntry(new byte[] {1});
            ledger.addEntry(new byte[] {2});
            assertThat(ledger.entryCache.getSize()).isPositive();
            assertThat(ledger.getReadEntriesCallbackExecutor()).isSameAs(ledger.getExecutor());

            Thread worker = blockWorker(ledger, releaseWorker);
            CompletableFuture<Thread> fromCaller = readOne(cursor, false);
            assertThat(fromCaller).isNotDone();
            releaseWorker.countDown();
            assertThat(fromCaller.get(10, TimeUnit.SECONDS)).isSameAs(worker);

            CompletableFuture<Boolean> queuedFromWorker = new CompletableFuture<>();
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
                queuedFromWorker.complete(!fromWorker.isDone());
            });
            assertThat(queuedFromWorker.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(fromWorker.get(10, TimeUnit.SECONDS)).isSameAs(worker);
        } finally {
            releaseWorker.countDown();
            ledger.close();
        }
    }

    @Test
    public void testCustomCallbackExecutorTakesPrecedenceForBothInlineSettings() throws Exception {
        ExecutorService callbackExecutor = Executors.newSingleThreadExecutor();
        try {
            Thread callbackThread = callbackExecutor.submit(Thread::currentThread).get(10, TimeUnit.SECONDS);
            for (boolean inline : List.of(false, true)) {
                ManagedLedgerConfig config = rawEntryConfig()
                        .setReadEntriesCallbackInline(inline)
                        .setReadEntriesCallbackExecutor(callbackExecutor);
                ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-custom-" + inline, config);
                try {
                    ManagedCursor cursor = ledger.openCursor("cursor");
                    ledger.addEntry(new byte[] {1});
                    assertThat(ledger.entryCache.getSize()).isPositive();
                    assertThat(ledger.getReadEntriesCallbackExecutor()).isSameAs(callbackExecutor);
                    assertThat(readOne(cursor, inline).get(10, TimeUnit.SECONDS)).isSameAs(callbackThread);
                } finally {
                    ledger.close();
                }
            }
        } finally {
            callbackExecutor.shutdownNow();
            assertThat(callbackExecutor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void testDirectCustomCallbackExecutorQueuesAtDepthLimitAndRestoresCustomAffinity() throws Exception {
        ExecutorService callbackExecutor = Executors.newSingleThreadExecutor();
        AtomicInteger executorCalls = new AtomicInteger();
        Executor directUntilTrampoline = command -> {
            if (executorCalls.incrementAndGet() <= OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 1) {
                command.run();
            } else {
                callbackExecutor.execute(command);
            }
        };
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-direct-custom",
                rawEntryConfig().setReadEntriesCallbackExecutor(directUntilTrampoline));
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            Thread callbackThread = callbackExecutor.submit(Thread::currentThread).get(10, TimeUnit.SECONDS);
            ManagedCursor cursor = ledger.openCursor("cursor");
            int count = OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 1;
            for (int i = 0; i < count; i++) {
                ledger.addEntry(new byte[] {(byte) i});
            }
            assertThat(ledger.entryCache.getSize()).isPositive();
            blockWorker(ledger, releaseWorker);
            Thread caller = Thread.currentThread();
            AtomicInteger completions = new AtomicInteger();
            AtomicReference<Thread> unexpectedThread = new AtomicReference<>();
            CompletableFuture<Thread> completed = new CompletableFuture<>();
            cursor.asyncReadEntries(1, new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    entries.forEach(Entry::release);
                    int completion = completions.incrementAndGet();
                    if (completion < count) {
                        if (Thread.currentThread() != caller) {
                            unexpectedThread.compareAndSet(null, Thread.currentThread());
                        }
                        cursor.asyncReadEntries(1, this, null, PositionFactory.LATEST);
                    } else {
                        completed.complete(Thread.currentThread());
                    }
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    completed.completeExceptionally(exception);
                }
            }, null, PositionFactory.LATEST);
            assertThat(completions.get()).isEqualTo(OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS);
            assertThat(unexpectedThread.get()).isNull();
            assertThat(completed).isNotDone();

            releaseWorker.countDown();
            assertThat(completed.get(10, TimeUnit.SECONDS)).isSameAs(callbackThread);
        } finally {
            releaseWorker.countDown();
            ledger.close();
            callbackExecutor.shutdownNow();
            assertThat(callbackExecutor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void testRejectedCustomExecutorReleasesEntriesAndCompletesFailureOnce() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        Executor rejectFirst = command -> {
            if (executions.getAndIncrement() == 0) {
                throw new RejectedExecutionException("test callback rejection");
            }
            command.run();
        };
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-custom-rejection",
                rawEntryConfig().setReadEntriesCallbackExecutor(rejectFirst));
        try {
            ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
            Position first = ledger.addEntry(new byte[] {1});
            Position second = ledger.addEntry(new byte[] {2});
            ledger.entryCache.clear();
            ByteBuf firstData = cacheEntry(ledger, first, (byte) 1);
            ByteBuf secondData = cacheEntry(ledger, second, (byte) 2);
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

            // Delivery rejection happens after the cursor advances. It reports failure and does not roll back the read.
            assertThat(readOne(cursor, false).get(10, TimeUnit.SECONDS)).isSameAs(Thread.currentThread());
            assertThat(secondData.refCnt()).isEqualTo(1);
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        } finally {
            ledger.close();
        }
    }

    @Test
    public void testRejectedCustomExecutorAfterDepthTrampolineReleasesEntriesAndCompletesFailureOnce()
            throws Exception {
        AtomicInteger executions = new AtomicInteger();
        Executor rejectAfterTrampoline = command -> {
            if (executions.incrementAndGet() > OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS + 1) {
                throw new RejectedExecutionException("test callback rejection after trampoline");
            }
            command.run();
        };
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-trampoline-rejection",
                rawEntryConfig().setReadEntriesCallbackExecutor(rejectAfterTrampoline));
        CountDownLatch releaseWorker = new CountDownLatch(1);
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
            Thread worker = blockWorker(ledger, releaseWorker);
            AtomicInteger completed = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            AtomicReference<ManagedLedgerException> failure = new AtomicReference<>();
            AtomicReference<Thread> failureThread = new AtomicReference<>();
            CompletableFuture<Void> failed = new CompletableFuture<>();
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
                    failureThread.set(Thread.currentThread());
                    failed.complete(null);
                }
            }, null, PositionFactory.LATEST);
            assertThat(completed.get()).isEqualTo(OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS);
            assertThat(failures.get()).isZero();
            assertThat(cursor.getPendingReadOpsCount()).isZero();

            releaseWorker.countDown();
            failed.get(10, TimeUnit.SECONDS);
            assertThat(failure.get()).isNotNull();
            assertThat(failureThread.get()).isSameAs(worker);
            assertThat(failures.get()).isEqualTo(1);
            assertThat(completed.get()).isEqualTo(OpReadEntry.MAX_NESTED_INLINE_COMPLETIONS);
            assertThat(cursor.getPendingReadOpsCount()).isZero();
            assertThat(rejectedData.refCnt()).isEqualTo(1); // Only the cache owns the rejected entry's buffer.
            assertThat(cursor.getReadPosition()).isEqualTo(rejectedPosition.getNext());
        } finally {
            releaseWorker.countDown();
            ledger.close();
        }
    }

    @Test
    public void testReadCompletionPolicyIsCapturedWhenLedgerOpens() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-captured-policy", rawEntryConfig());
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            ManagedCursor cursor = ledger.openCursor("cursor");
            ledger.addEntry(new byte[] {1});
            assertThat(ledger.getReadEntriesCallbackExecutor()).isSameAs(ledger.getExecutor());
            ledger.setConfig(rawEntryConfig()
                    .setReadEntriesCallbackInline(true)
                    .setReadEntriesCallbackExecutor(Runnable::run));
            assertThat(ledger.getReadEntriesCallbackExecutor()).isSameAs(ledger.getExecutor());

            Thread worker = blockWorker(ledger, releaseWorker);
            CompletableFuture<Thread> callback = readOne(cursor, false);
            assertThat(callback).isNotDone();
            releaseWorker.countDown();
            assertThat(callback.get(10, TimeUnit.SECONDS)).isSameAs(worker);
        } finally {
            releaseWorker.countDown();
            ledger.close();
        }
    }

    @Test
    public void testLedgerCloseDoesNotCloseCustomCallbackExecutor() throws Exception {
        ExecutorService callbackExecutor = Executors.newSingleThreadExecutor();
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("completion-custom-owner",
                rawEntryConfig().setReadEntriesCallbackExecutor(callbackExecutor));
        try {
            ledger.close();
            assertThat(callbackExecutor.isShutdown()).isFalse();
        } finally {
            callbackExecutor.shutdownNow();
            assertThat(callbackExecutor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
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

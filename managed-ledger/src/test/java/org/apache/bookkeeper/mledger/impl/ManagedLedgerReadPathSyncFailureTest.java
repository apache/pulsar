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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.merlimat.slog.Logger;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Regression tests for synchronous failures escaping the read path of the managed ledger: they must
 * be reported through the callbacks instead, so that the pending read operations counter is undone
 * and the replay's aggregate callback can complete.
 */
public class ManagedLedgerReadPathSyncFailureTest extends MockedBookKeeperTestCase {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final List<Entry> entries = new CopyOnWriteArrayList<>();
    private final AtomicReference<ManagedLedgerException> failure = new AtomicReference<>();

    @AfterMethod(alwaysRun = true)
    public void afterMethod() {
        validateMockitoUsage();
        entries.forEach(Entry::release);
        entries.clear();
        failure.set(null);
    }

    /**
     * On a batch read, the wrapper carries a ReadEntriesCallback: a failure to register its
     * timeout must reach that callback with the original cause, and a late storage completion
     * must release its entries without completing the wrapper again.
     */
    @Test
    public void testBatchReadTimeoutRegistrationFailureNotifiesBatchCallback() throws Exception {
        CompletableFuture<Void> releaseReads = new CompletableFuture<>();
        AtomicBoolean readDelivered = new AtomicBoolean();
        bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, readEntries) ->
                releaseReads.thenApply(__ -> {
                    readDelivered.set(true);
                    return readEntries;
                }));

        ManagedLedgerFactoryImpl factorySpy = spy(factory);
        ReadEntryTimeoutTracker tracker = spy(factory.getReadEntryTimeoutTracker());
        RuntimeException cause = new RuntimeException("timeout registration failed");
        doThrow(cause).doCallRealMethod()
                .when(tracker).add(any(ManagedLedgerImpl.ReadEntryCallbackWrapper.class));
        doReturn(tracker).when(factorySpy).getReadEntryTimeoutTracker();

        ManagedLedgerConfig config =
                initManagedLedgerConfig(new ManagedLedgerConfig()).setReadEntryTimeoutSeconds(60);
        ManagedLedgerImpl ledger =
                (ManagedLedgerImpl) factorySpy.open("sync-failure-batch-timeout-registration", config);
        ManagedCursorImpl cursor =
                (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);
        cursor.setInactive();
        ledger.addEntry("entry-0".getBytes(UTF_8));

        AtomicInteger failures = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();
        cursor.asyncReadEntriesOrWait(1, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> readEntries, Object ctx) {
                completions.incrementAndGet();
                entries.addAll(readEntries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                failures.incrementAndGet();
                failure.set(exception);
            }
        }, null, ledger.getLastConfirmedEntry());

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failure.get().getCause()).isSameAs(cause);
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        });
        // The late storage completion releases its entries without a second notification: the
        // completion guard was consumed by the registration failure
        releaseReads.complete(null);
        await().atMost(TIMEOUT).untilTrue(readDelivered);
        await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failures).hasValue(1);
            assertThat(completions).hasValue(0);
            assertThat(entries).isEmpty();
        });
    }

    /**
     * When the read timeout fires while the executor is shutting down and rejects the notification
     * task, the wrapper must notify the callback inline: it is already COMPLETED, and nobody else
     * would ever notify it, leaving the read pending forever.
     */
    @Test
    public void testReadTimeoutNotifiesInlineWhenExecutorRejects() {
        ThreadBoundExecutor rejectingExecutor = mock(ThreadBoundExecutor.class);
        doThrow(new RejectedExecutionException("shutting down")).when(rejectingExecutor).execute(any());
        ManagedLedgerImpl rejectedLedger = mock(ManagedLedgerImpl.class);
        doReturn(rejectingExecutor).when(rejectedLedger).getExecutor();
        ReadEntriesCallback batchCallback = mock(ReadEntriesCallback.class);
        ManagedLedgerImpl.ReadEntryCallbackWrapper wrapper = ManagedLedgerImpl.ReadEntryCallbackWrapper.create(
                rejectedLedger, 1L, 1L, batchCallback, null, Long.MAX_VALUE);

        Assert.assertTrue(wrapper.registerTimeout());
        Assert.assertTrue(wrapper.triggerReadTimeout(
                new ManagedLedgerException("read timed out")));

        verify(batchCallback, timeout(1_000))
                .readEntriesFailed(any(ManagedLedgerException.class), any());
    }

    private ReadEntriesCallback callback() {
        return new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> readEntries, Object ctx) {
                entries.addAll(readEntries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                failure.set(exception);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> RuntimeException sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }

    /**
     * A skip condition that throws while the read scans the entries must be reported through
     * {@code readEntriesFailed} and must not leave the pending read operations counter above zero,
     * which would hold back every mark delete until the next cursor ledger rollover.
     */
    @Test
    public void testReadPathFailureIsReportedThroughCallback() throws Exception {
        ManagedLedger ledger = factory.open("sync-failure-read-path");
        ManagedCursorImpl cursor =
                (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);
        ledger.addEntry("entry-0".getBytes(UTF_8));

        Predicate<Position> skipCondition = position -> {
            throw new RuntimeException("skip condition failed");
        };
        cursor.asyncReadEntriesWithSkipOrWait(1, callback(), null,
                ledger.getLastConfirmedEntry(), skipCondition);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failure.get()).isNotNull();
            assertThat(entries).isEmpty();
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        });
    }

    /**
     * A failure to start the read ahead runs after the read it follows was already submitted: it
     * must not fail that read.
     */
    @Test
    public void testReadAheadStartFailureDoesNotFailTheRead() throws Exception {
        ManagedLedger ledger = factory.open("sync-failure-read-ahead");
        // Spied so the read ahead start can be made to fail. The read operation carries the cursor,
        // so its read ahead runs against this instance.
        ManagedCursorImpl cursor = spy((ManagedCursorImpl) ledger.openCursor("c"));
        ledger.addEntry("entry-0".getBytes(UTF_8));
        ledger.addEntry("entry-1".getBytes(UTF_8));

        doThrow(new RuntimeException("read ahead start failed")).when(cursor).tryStartReadAhead();

        cursor.asyncReadEntriesOrWait(1, callback(), null, ledger.getLastConfirmedEntry());

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(entries).hasSize(1);
            assertThat(failure.get()).isNull();
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        });
    }

    /**
     * A failure to register the read timeout runs after the read was already submitted: it must be
     * reported through the wrapper, whose completion guard also coordinates the storage callbacks,
     * instead of failing the read operation directly.
     */
    @Test
    public void testTimeoutRegistrationFailureIsReportedThroughTheWrapper() throws Exception {
        // Hold the storage read, so the failure to register the timeout reaches the wrapper
        // before the read completes
        CompletableFuture<Void> releaseReads = new CompletableFuture<>();
        bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, readEntries) ->
                releaseReads.thenApply(__ -> readEntries));

        ManagedLedgerFactoryImpl factorySpy = spy(factory);
        ReadEntryTimeoutTracker tracker = spy(factory.getReadEntryTimeoutTracker());
        doThrow(new RuntimeException("timeout registration failed")).doCallRealMethod()
                .when(tracker).add(any(ManagedLedgerImpl.ReadEntryCallbackWrapper.class));
        doReturn(tracker).when(factorySpy).getReadEntryTimeoutTracker();

        ManagedLedgerConfig config =
                initManagedLedgerConfig(new ManagedLedgerConfig()).setReadEntryTimeoutSeconds(60);
        ManagedLedger ledger = factorySpy.open("sync-failure-timeout-registration", config);
        ManagedCursorImpl cursor =
                (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);
        cursor.setInactive();
        ledger.addEntry("entry-0".getBytes(UTF_8));

        cursor.asyncReadEntriesOrWait(1, callback(), null, ledger.getLastConfirmedEntry());

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failure.get()).isNotNull();
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        });
        releaseReads.complete(null);
    }

    /**
     * A failure to issue one of the replay's reads must fail the positions whose read was not
     * issued through the aggregate callback, so that the callback completes instead of hanging with
     * the entries it already collected.
     */
    @Test
    public void testReplayReadIssueFailureCompletesTheCallback() throws Exception {
        // The entries are written before the spy takes its field snapshot, so the spy and the
        // managed ledger the entry cache holds agree on the last confirmed entry.
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("sync-failure-replay");
        Position p0 = ledger.addEntry("entry-0".getBytes(UTF_8));
        Position p1 = ledger.addEntry("entry-1".getBytes(UTF_8));
        Position p2 = ledger.addEntry("entry-2".getBytes(UTF_8));
        ledger = spy(ledger);
        ManagedCursorImpl cursor =
                (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);

        // Fail the issue of the replay's reads, whether they are cached reads, single storage reads
        // or one range read of a run.
        doThrow(new RuntimeException("replay read issue failed"))
                .when(ledger).asyncReadEntry(any(Position.class), anyInt(), any(), any());
        doThrow(new RuntimeException("replay read issue failed"))
                .when(ledger).asyncReadEntries(anyLong(), anyLong(), anyLong(), anyInt(), anyInt(), any(), any());

        Set<Position> positions = new HashSet<>(List.of(p0, p1, p2));
        cursor.asyncReplayEntries(positions, callback(), null);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failure.get()).isNotNull();
            assertThat(entries).isEmpty();
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        });
    }

    /**
     * A synchronous failure while building the read operation - for example an allocation failure - must be
     * reported through {@code readEntriesFailed} instead of escaping to the caller, and must not leave the
     * pending read operations counter above zero.
     */
    @Test
    public void testReadOpCreationFailureIsReportedThroughCallback() throws Exception {
        ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
        when(ledger.getConfig()).thenReturn(new ManagedLedgerConfig());
        when(ledger.getLogger()).thenReturn(Logger.get(ManagedLedgerImpl.class));
        when(ledger.getLastPosition()).thenReturn(PositionFactory.create(1, 0));
        doThrow(new RuntimeException("read op creation failed"))
                .when(ledger).startReadOperationOnLedger(any(Position.class));
        ManagedCursorImpl cursor = new ManagedCursorImpl(mock(BookKeeper.class), ledger, "c");
        cursor.readPosition = PositionFactory.create(1, 0);

        cursor.asyncReadEntriesOrWait(1, callback(), null, PositionFactory.LATEST);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failure.get()).isNotNull();
            assertThat(entries).isEmpty();
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        });
    }

    /**
     * The wait path - a read deferred until entries are available - also builds a read operation. A synchronous
     * failure there must be reported through the callback too, and must not leave a waiting read operation behind.
     */
    @Test
    public void testWaitPathSyncFailureIsReportedThroughCallback() throws Exception {
        ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
        when(ledger.getConfig()).thenReturn(new ManagedLedgerConfig());
        when(ledger.getLogger()).thenReturn(Logger.get(ManagedLedgerImpl.class));
        // No entries: the cursor's read position is past the ledger's last position, so the read takes the wait path.
        when(ledger.getLastPosition()).thenReturn(PositionFactory.create(1, 0));
        doThrow(new RuntimeException("read op creation failed"))
                .when(ledger).startReadOperationOnLedger(any(Position.class));
        ManagedCursorImpl cursor = new ManagedCursorImpl(mock(BookKeeper.class), ledger, "c");
        cursor.readPosition = PositionFactory.create(1, 1);

        cursor.asyncReadEntriesOrWait(1, callback(), null, PositionFactory.LATEST);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failure.get()).isNotNull();
            assertThat(cursor.getPendingReadOpsCount()).isZero();
            assertThat(cursor.hasPendingReadRequest()).isFalse();
        });
    }

    /**
     * A read-path failure that is already a classified {@link ManagedLedgerException} must reach the callback
     * unwrapped, so that downstream handling - auto-skip and throttling - can still discriminate on the type.
     */
    @Test
    public void testReadPathFailurePreservesManagedLedgerExceptionType() throws Exception {
        ManagedLedger ledger = factory.open("sync-failure-exception-type");
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);
        ledger.addEntry("entry-0".getBytes(UTF_8));

        ManagedLedgerException.NonRecoverableLedgerException thrown =
                new ManagedLedgerException.NonRecoverableLedgerException("no ledger exist");
        Predicate<Position> skipCondition = position -> {
            throw sneakyThrow(thrown);
        };
        cursor.asyncReadEntriesWithSkipOrWait(1, callback(), null,
                ledger.getLastConfirmedEntry(), skipCondition);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failure.get()).isSameAs(thrown);
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        });
    }

    /**
     * When auto-skip moves a failed read to the next valid ledger, the pending read operations counter must stay
     * balanced: the failed read and its continuation are one pending operation, completed once. Before the fix the
     * continuation's completion decremented the counter a second time, leaving it negative.
     */
    @Test(timeOut = 30000)
    public void testAutoSkipContinuationKeepsPendingReadOpsBalanced() throws Exception {
        ManagedLedgerConfig config =
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1);
        config.setAutoSkipNonRecoverableData(true);
        ManagedLedger ledger = factory.open("auto-skip-pending-read-ops", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);
        // Disable caching when adding entries, so the reads go to the bookie.
        cursor.setInactive();
        Position p0 = ledger.addEntry("entry-0".getBytes(UTF_8));
        ledger.addEntry("entry-1".getBytes(UTF_8));
        bkc.deleteLedger(p0.getLedgerId());

        CompletableFuture<List<Entry>> read = new CompletableFuture<>();
        cursor.asyncReadEntries(1, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> readEntries, Object ctx) {
                read.complete(readEntries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                read.completeExceptionally(exception);
            }
        }, null, PositionFactory.LATEST);

        List<Entry> readEntries = read.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        readEntries.forEach(Entry::release);
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(cursor.getPendingReadOpsCount()).isZero());
    }

    /**
     * A replay of positions that were all acknowledged has nothing to read: it must complete through the callback
     * instead of leaving the caller waiting forever for an aggregate that counts zero positions.
     */
    @Test
    public void testReplayOfAlreadyAcknowledgedPositionsCompletes() throws Exception {
        ManagedLedger ledger = factory.open("replay-all-acked");
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);
        Position position = ledger.addEntry("entry-0".getBytes(UTF_8));
        cursor.markDelete(position);

        AtomicInteger completions = new AtomicInteger();
        Set<Position> alreadyAcknowledged = new HashSet<>(cursor.asyncReplayEntries(Set.of(position),
                new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> readEntries, Object ctx) {
                completions.incrementAndGet();
                entries.addAll(readEntries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                completions.incrementAndGet();
                failure.set(exception);
            }
        }, null));

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(completions).hasValue(1));
        assertThat(entries).isEmpty();
        assertThat(failure.get()).isNull();
        assertThat(alreadyAcknowledged).containsExactly(position);
    }

    /**
     * When a replay's reads fail partway, the positions whose read was already issued must not be failed again
     * through the failure path: the aggregate callback counts every position exactly once. The first position's
     * read completes, the second one's fails, and the third one's succeeds afterwards: the aggregate still fails
     * exactly once and releases the entries it had collected.
     */
    @Test
    public void testReplayReadIssueFailureDoesNotDoubleFailIssuedPositions() throws Exception {
        // One entry per ledger, so each replay position is its own storage read, and the second one can be
        // failed independently. The cursor is inactive, so the entries are not cached.
        ManagedLedgerConfig config =
                initManagedLedgerConfig(new ManagedLedgerConfig()).setMaxEntriesPerLedger(1);
        ManagedLedger ledger = factory.open("replay-partial-failure", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("c", InitialPosition.Earliest);
        cursor.setInactive();
        Position p0 = ledger.addEntry("entry-0".getBytes(UTF_8));
        Position p1 = ledger.addEntry("entry-1".getBytes(UTF_8));
        Position p2 = ledger.addEntry("entry-2".getBytes(UTF_8));
        AtomicInteger reads = new AtomicInteger();
        bkc.setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, readEntries) ->
                reads.incrementAndGet() == 2
                        ? CompletableFuture.failedFuture(new RuntimeException("replay read issue failed"))
                        : CompletableFuture.completedFuture(readEntries));

        AtomicInteger completions = new AtomicInteger();
        cursor.asyncReplayEntries(new LinkedHashSet<>(List.of(p0, p1, p2)), new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> readEntries, Object ctx) {
                completions.incrementAndGet();
                entries.addAll(readEntries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                completions.incrementAndGet();
                failure.set(exception);
            }
        }, null);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(failure.get()).isNotNull();
            assertThat(completions).hasValue(1);
            assertThat(entries).isEmpty();
            assertThat(cursor.getPendingReadOpsCount()).isZero();
        });
    }
}

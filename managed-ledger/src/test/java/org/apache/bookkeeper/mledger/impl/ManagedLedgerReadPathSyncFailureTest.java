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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.validateMockitoUsage;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
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
}

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class ManagedLedgerReadHandleLifecycleTest extends MockedBookKeeperTestCase {
    private final List<ReadHandle> openedHandles = new CopyOnWriteArrayList<>();
    private CompletableFuture<Void> openCompletion;

    private enum CloseOutcome {
        NORMAL, FENCED, WRITE_HANDLE_FAILURE
    }

    private record LedgerFixture(ManagedLedgerImpl writer, List<Position> positions) {
    }

    @Override
    protected void setUpTestCase() throws Exception {
        openedHandles.clear();
        openCompletion = CompletableFuture.completedFuture(null);
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        bkc = spy(bkc);
        doAnswer(invocation -> trackOpens((OpenBuilder) invocation.callRealMethod()))
                .when(bkc).newOpenLedgerOp();
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
    }

    @Override
    protected void cleanUpTestCase() throws Exception {
        openCompletion.complete(null);
        for (ReadHandle handle : openedHandles) {
            handle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    private OpenBuilder trackOpens(OpenBuilder builder) {
        OpenBuilder trackedBuilder = spy(builder);
        doAnswer(invocation -> ((CompletableFuture<ReadHandle>) invocation.callRealMethod()).thenCompose(handle -> {
            ReadHandle trackedHandle = spy(handle);
            openedHandles.add(trackedHandle);
            return openCompletion.thenApply(ignored -> trackedHandle);
        })).when(trackedBuilder).execute();
        return trackedBuilder;
    }

    private LedgerFixture openLedger(String name) throws Exception {
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.DAYS)
                .setRetentionSizeInMB(-1).setMaxEntriesPerLedger(2);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl writer = (ManagedLedgerImpl) factory.open(name, config);
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            positions.add(writer.addEntry(new byte[] {(byte) (i + 1)}));
        }
        assertThat(positions.get(0).getLedgerId()).isNotEqualTo(positions.get(2).getLedgerId());
        assertThat(positions.get(2).getLedgerId()).isNotEqualTo(positions.get(4).getLedgerId());
        assertThat(writer.getState()).isEqualTo(ManagedLedgerImpl.State.LedgerOpened);
        return new LedgerFixture(writer, positions);
    }

    private ManagedLedgerImpl reader(LedgerFixture fixture, boolean readOnly) throws Exception {
        if (!readOnly) {
            return fixture.writer();
        }
        ReadOnlyManagedLedgerImpl ledger = new ReadOnlyManagedLedgerImpl(factory, bkc, factory.getMetaStore(),
                fixture.writer().getConfig(), executor, fixture.writer().getName());
        ledger.initialize().get(5, TimeUnit.SECONDS);
        return ledger;
    }

    @DataProvider(name = "closeOutcomes")
    public Object[][] closeOutcomes() {
        return new Object[][] {{CloseOutcome.NORMAL}, {CloseOutcome.FENCED}, {CloseOutcome.WRITE_HANDLE_FAILURE}};
    }

    @DataProvider(name = "readerKinds")
    public Object[][] readerKinds() {
        return new Object[][] {{false}, {true}};
    }

    @Test
    public void testSynchronousOpenFailureIsReturnedAsFailedFuture() throws Exception {
        LedgerFixture fixture = openLedger("synchronous-read-open-failure");
        doAnswer(invocation -> {
            throw new IllegalStateException("open failed synchronously");
        }).when(bkc).newOpenLedgerOp();

        CompletableFuture<ReadHandle> opening = fixture.writer()
                .getLedgerHandle(fixture.positions().get(0).getLedgerId());

        assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                .hasRootCauseMessage("open failed synchronously");
        assertThat(fixture.writer().ledgerCache.size()).isZero();
    }

    @Test(dataProvider = "closeOutcomes")
    public void testWritableCloseReleasesReadHandlesOnEveryExit(CloseOutcome outcome) throws Exception {
        LedgerFixture fixture = openLedger("writable-handle-close");
        ManagedLedgerImpl ledger = fixture.writer();
        long firstLedgerId = fixture.positions().get(0).getLedgerId();
        ReadHandle first = ledger.getLedgerHandle(firstLedgerId).get(5, TimeUnit.SECONDS);
        ReadHandle second = ledger.getLedgerHandle(fixture.positions().get(2).getLedgerId()).get(5, TimeUnit.SECONDS);
        AtomicBoolean closeHeldMonitor = new AtomicBoolean();
        for (ReadHandle handle : List.of(first, second)) {
            doAnswer(invocation -> {
                if (Thread.holdsLock(ledger)) {
                    closeHeldMonitor.set(true);
                }
                return invocation.callRealMethod();
            }).when(handle).closeAsync();
        }

        if (outcome == CloseOutcome.FENCED) {
            ledger.setFenced();
            assertThatThrownBy(ledger::close).isInstanceOf(ManagedLedgerFencedException.class);
        } else if (outcome == CloseOutcome.WRITE_HANDLE_FAILURE) {
            bkc.failNow(BKException.Code.WriteException);
            assertThatThrownBy(ledger::close).isInstanceOf(ManagedLedgerException.class)
                    .hasMessageContaining("error code: " + BKException.Code.WriteException);
        } else {
            ledger.close();
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(first).closeAsync();
            verify(second).closeAsync();
        });
        assertThat(closeHeldMonitor).isFalse();
        assertThat(ledger.ledgerCache.size()).isZero();
        clearInvocations(bkc);
        assertThatThrownBy(() -> ledger.getLedgerHandle(firstLedgerId).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
        assertThatThrownBy(() -> ledger.reopenReadHandle(firstLedgerId).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
        verify(bkc, never()).newOpenLedgerOp();
        assertThat(ledger.ledgerCache.size()).isZero();
        if (outcome == CloseOutcome.FENCED) {
            assertThatThrownBy(ledger::close).isInstanceOf(ManagedLedgerFencedException.class);
        } else {
            ledger.close();
        }
        verify(first).closeAsync();
        verify(second).closeAsync();
    }

    @Test(dataProvider = "readerKinds")
    public void testPendingOpenIsClosedWithoutDelayingLogicalClose(boolean readOnly) throws Exception {
        LedgerFixture fixture = openLedger("pending-handle-close");
        ManagedLedgerImpl ledger = reader(fixture, readOnly);
        int baseline = openedHandles.size();
        openCompletion = new CompletableFuture<>();
        CompletableFuture<ReadHandle> opening = ledger.getLedgerHandle(fixture.positions().get(0).getLedgerId());
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> openedHandles.size() > baseline);
        ReadHandle handle = openedHandles.get(baseline);
        assertThat(opening).isNotDone();

        Futures.CloseFuture closing = new Futures.CloseFuture();
        ledger.asyncClose(closing, null);
        closing.get(5, TimeUnit.SECONDS);
        assertThat(opening).isNotDone();
        assertThat(ledger.ledgerCache.size()).isZero();
        verify(handle, never()).closeAsync();

        openCompletion.complete(null);
        opening.get(5, TimeUnit.SECONDS);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(handle).closeAsync());
    }

    @Test(dataProvider = "readerKinds")
    public void testCloseDuringHandleOpenRejectsResult(boolean readOnly) throws Exception {
        LedgerFixture fixture = openLedger("late-handle-close");
        ManagedLedgerImpl ledger = reader(fixture, readOnly);
        int baseline = openedHandles.size();
        CompletableFuture<Void> openStarted = new CompletableFuture<>();
        CompletableFuture<Void> allowOpen = new CompletableFuture<>();
        doAnswer(invocation -> {
            openStarted.complete(null);
            allowOpen.get(5, TimeUnit.SECONDS);
            return trackOpens((OpenBuilder) invocation.callRealMethod());
        }).when(bkc).newOpenLedgerOp();
        CompletableFuture<CompletableFuture<ReadHandle>> openingAttempt = new CompletableFuture<>();
        Thread openingThread = new Thread(() -> {
            try {
                openingAttempt.complete(ledger.getLedgerHandle(fixture.positions().get(0).getLedgerId()));
            } catch (Throwable error) {
                openingAttempt.completeExceptionally(error);
            }
        }, "managed-ledger-read-handle-opening");
        CompletableFuture<Void> closing = new CompletableFuture<>();
        Thread closingThread = new Thread(() -> {
            try {
                ledger.close();
                closing.complete(null);
            } catch (Throwable error) {
                closing.completeExceptionally(error);
            }
        }, "managed-ledger-read-handle-closing");
        try {
            openingThread.start();
            openStarted.get(5, TimeUnit.SECONDS);
            closingThread.start();
            // The pending promise is registered before the backend open. Observe close before allowing the open.
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(ledger::isClosingOrClosed);
            allowOpen.complete(null);

            CompletableFuture<ReadHandle> opening = openingAttempt.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
            closing.get(5, TimeUnit.SECONDS);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> openedHandles.size() > baseline);
            ReadHandle handle = openedHandles.get(baseline);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(handle).closeAsync());
            assertThat(ledger.ledgerCache.size()).isZero();
        } finally {
            allowOpen.complete(null);
            openingThread.join(TimeUnit.SECONDS.toMillis(5));
            closingThread.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(openingThread.isAlive()).isFalse();
            assertThat(closingThread.isAlive()).isFalse();
        }
    }

    @Test(dataProvider = "readerKinds")
    public void testReadFailureInvalidatesItsOwnHandle(boolean readOnly) throws Exception {
        LedgerFixture fixture = openLedger("failed-read-handle");
        ManagedLedgerImpl ledger = reader(fixture, readOnly);
        long ledgerId = fixture.positions().get(0).getLedgerId();
        ReadHandle failed = ledger.getLedgerHandle(ledgerId).get(5, TimeUnit.SECONDS);

        ledger.invalidateLedgerHandle(failed);

        assertThat(ledger.ledgerCache.get(ledgerId)).isNull();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(failed).closeAsync());
        ReadHandle replacement = ledger.getLedgerHandle(ledgerId).get(5, TimeUnit.SECONDS);
        assertThat(replacement).isNotSameAs(failed);
        ledger.close();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(replacement).closeAsync());
    }

    @Test(dataProvider = "readerKinds")
    public void testStaleReadFailurePreservesReplacementHandle(boolean readOnly) throws Exception {
        LedgerFixture fixture = openLedger("stale-read-handle");
        ManagedLedgerImpl ledger = reader(fixture, readOnly);
        long ledgerId = fixture.positions().get(0).getLedgerId();
        ReadHandle stale = ledger.getLedgerHandle(ledgerId).get(5, TimeUnit.SECONDS);
        ReadHandle replacement = ledger.reopenReadHandle(ledgerId).get(5, TimeUnit.SECONDS);
        assertThat(replacement).isNotSameAs(stale);
        CompletableFuture<ReadHandle> replacementFuture = ledger.ledgerCache.get(ledgerId);

        // A delayed failure still refers to the handle that reopen already replaced.
        ledger.invalidateLedgerHandle(stale);

        assertThat(ledger.ledgerCache.get(ledgerId)).isSameAs(replacementFuture);
        assertThat(ledger.getLedgerHandle(ledgerId).get(5, TimeUnit.SECONDS)).isSameAs(replacement);
        verify(replacement, never()).closeAsync();
        ledger.close();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(replacement).closeAsync());
    }

    @Test
    public void testTerminationAllowsHistoricalReadsUntilExplicitClose() throws Exception {
        LedgerFixture fixture = openLedger("terminated-read-handle");
        ManagedLedgerImpl ledger = fixture.writer();
        ledger.terminate();
        assertThat(ledger.getState()).isEqualTo(ManagedLedgerImpl.State.Terminated);
        Position position = fixture.positions().get(0);
        ReadHandle handle = ledger.getLedgerHandle(position.getLedgerId()).get(5, TimeUnit.SECONDS);
        ledger.entryCache.clear();
        CompletableFuture<Entry> reading = new CompletableFuture<>();
        ledger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                reading.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                reading.completeExceptionally(exception);
            }
        }, null);
        Entry entry = reading.get(5, TimeUnit.SECONDS);
        try {
            assertThat(entry.getData()).isEqualTo(new byte[] {1});
        } finally {
            entry.release();
        }
        verify(handle, never()).closeAsync();
        ledger.close();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(handle).closeAsync());
    }
}

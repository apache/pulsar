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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFactoryClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedgerImplWrapper;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(timeOut = 30000)
public class ReadOnlyManagedLedgerFactoryLifecycleTest extends MockedBookKeeperTestCase {
    private final List<ReadHandle> openedHandles = new CopyOnWriteArrayList<>();
    private CompletableFuture<BookKeeper> bookKeeperAccess;
    private CompletableFuture<Long> lastAddConfirmedGate;
    private TrackingFactory trackingFactory;

    private record TrackedLedger(ReadOnlyManagedLedgerImplWrapper ledger, AtomicInteger closeCallbacks) {
    }

    private static class OpenAttempt implements AsyncCallbacks.OpenReadOnlyManagedLedgerCallback {
        private final CompletableFuture<ReadOnlyManagedLedger> result = new CompletableFuture<>();
        private final AtomicInteger successes = new AtomicInteger();
        private final AtomicInteger failures = new AtomicInteger();

        @Override
        public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger, Object ctx) {
            successes.incrementAndGet();
            result.complete(managedLedger);
        }

        @Override
        public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
            failures.incrementAndGet();
            result.completeExceptionally(exception);
        }
    }

    private static class TrackingFactory extends ManagedLedgerFactoryImpl {
        private final List<TrackedLedger> createdLedgers = new CopyOnWriteArrayList<>();
        private final CompletableFuture<Void> initialized = new CompletableFuture<>();
        private CompletableFuture<Void> initializationGate = CompletableFuture.completedFuture(null);

        TrackingFactory(MetadataStoreExtended store, BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory)
                throws Exception {
            this(store, bkFactory, new ManagedLedgerFactoryConfig());
        }

        TrackingFactory(MetadataStoreExtended store, BookkeeperFactoryForCustomEnsemblePlacementPolicy bkFactory,
                        ManagedLedgerFactoryConfig config) throws Exception {
            super(store, bkFactory, config);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected ReadOnlyManagedLedgerImplWrapper createReadOnlyManagedLedger(BookKeeper bk, String name,
                                                                               ManagedLedgerConfig config,
                                                                               Runnable onClosed) {
            AtomicInteger closeCallbacks = new AtomicInteger();
            ReadOnlyManagedLedgerImplWrapper ledger = spy(super.createReadOnlyManagedLedger(bk, name, config, () -> {
                closeCallbacks.incrementAndGet();
                onClosed.run();
            }));
            createdLedgers.add(new TrackedLedger(ledger, closeCallbacks));
            doAnswer(invocation -> ((CompletableFuture<Void>) invocation.callRealMethod()).thenCompose(__ -> {
                initialized.complete(null);
                return initializationGate;
            })).when(ledger).initialize();
            return ledger;
        }
    }

    @Override
    protected void setUpTestCase() throws Exception {
        openedHandles.clear();
        lastAddConfirmedGate = null;
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        bkc = spy(bkc);
        doAnswer(invocation -> trackOpens((OpenBuilder) invocation.callRealMethod()))
                .when(bkc).newOpenLedgerOp();
        bookKeeperAccess = CompletableFuture.completedFuture(bkc);
        trackingFactory = new TrackingFactory(metadataStore, policy -> bookKeeperAccess);
        factory = trackingFactory;
    }

    @Override
    protected void cleanUpTestCase() throws Exception {
        bookKeeperAccess.complete(bkc);
        trackingFactory.initializationGate.complete(null);
        if (lastAddConfirmedGate != null) {
            lastAddConfirmedGate.complete(0L);
        }
        for (ReadHandle handle : openedHandles) {
            handle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    private OpenBuilder trackOpens(OpenBuilder builder) {
        OpenBuilder tracked = spy(builder);
        doAnswer(invocation -> ((CompletableFuture<ReadHandle>) invocation.callRealMethod()).thenApply(handle -> {
            ReadHandle trackedHandle = spy(handle);
            if (lastAddConfirmedGate != null) {
                doReturn(lastAddConfirmedGate).when(trackedHandle).readLastAddConfirmedAsync();
            }
            openedHandles.add(trackedHandle);
            return trackedHandle;
        })).when(tracked).execute();
        return tracked;
    }

    private ManagedLedgerImpl writer(String name) throws Exception {
        ManagedLedgerImpl writer = (ManagedLedgerImpl) factory.open(name,
                defaultConfig().setRetentionTime(1, TimeUnit.DAYS));
        writer.addEntry(new byte[] {1});
        return writer;
    }

    private OpenAttempt beginOpen(ManagedLedgerImpl writer) {
        OpenAttempt open = new OpenAttempt();
        factory.asyncOpenReadOnlyManagedLedger(writer.getName(), open, writer.getConfig(), null);
        return open;
    }

    private ReadOnlyManagedLedger open(ManagedLedgerImpl writer) throws Exception {
        return beginOpen(writer).result.get(5, TimeUnit.SECONDS);
    }

    private static CompletableFuture<Entry> read(ReadOnlyManagedLedger ledger, Position position) {
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
        return reading;
    }

    private ReadHandle readAndCaptureHandle(ReadOnlyManagedLedger ledger, Position position) throws Exception {
        int baseline = openedHandles.size();
        read(ledger, position).get(5, TimeUnit.SECONDS).release();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> openedHandles.size() > baseline);
        return openedHandles.get(baseline);
    }

    @Test
    public void testSameNameReadOnlyLedgersCloseIndependentlyOfWriter() throws Exception {
        ManagedLedgerImpl writer = writer("independent-read-only-ledgers");
        Position position = writer.getLastConfirmedEntry();
        var writerCache = writer.entryCache;
        ReadOnlyManagedLedger first = open(writer);
        ReadOnlyManagedLedger second = open(writer);
        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isEqualTo(2);
        ReadHandle firstHandle = readAndCaptureHandle(first, position);
        ReadHandle secondHandle = readAndCaptureHandle(second, position);
        assertThat(firstHandle).isNotSameAs(secondHandle);

        first.asyncClose().get(5, TimeUnit.SECONDS);
        first.asyncClose().get(5, TimeUnit.SECONDS);

        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isEqualTo(1);
        assertThat(trackingFactory.createdLedgers.get(0).closeCallbacks()).hasValue(1);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(firstHandle).closeAsync());
        verify(secondHandle, never()).closeAsync();
        read(second, position).get(5, TimeUnit.SECONDS).release();
        assertThat(factory.open(writer.getName(), writer.getConfig())).isSameAs(writer);
        assertThat(factory.getEntryCacheManager().getEntryCache(writer)).isSameAs(writerCache);
        writer.addEntry(new byte[] {2});

        second.asyncClose().get(5, TimeUnit.SECONDS);

        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
        assertThat(trackingFactory.createdLedgers.get(1).closeCallbacks()).hasValue(1);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(secondHandle).closeAsync());
        assertThat(factory.open(writer.getName(), writer.getConfig())).isSameAs(writer);
        writer.addEntry(new byte[] {3});
    }

    @Test
    public void testFactoryShutdownClosesForgottenReadOnlyLedgers() throws Exception {
        ManagedLedgerImpl writer = writer("forgotten-read-only-ledgers");
        ReadOnlyManagedLedger first = open(writer);
        ReadOnlyManagedLedger second = open(writer);
        ReadHandle firstHandle = readAndCaptureHandle(first, writer.getLastConfirmedEntry());
        ReadHandle secondHandle = readAndCaptureHandle(second, writer.getLastConfirmedEntry());

        factory.shutdownAsync().get(5, TimeUnit.SECONDS);

        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
        for (TrackedLedger tracked : trackingFactory.createdLedgers) {
            assertThat(tracked.closeCallbacks()).hasValue(1);
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(firstHandle).closeAsync();
            verify(secondHandle).closeAsync();
        });
        assertThatThrownBy(() -> read(first, writer.getLastConfirmedEntry()).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
    }

    @Test
    public void testShutdownDoesNotWaitForReadHandleOpen() throws Exception {
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setNumManagedLedgerSchedulerThreads(1);
        trackingFactory = new TrackingFactory(metadataStore, policy -> bookKeeperAccess,
                factoryConfig);
        factory = trackingFactory;
        ManagedLedgerImpl writer = writer("read-only-shutdown-blocked-open");
        Position position = writer.getLastConfirmedEntry();
        OpenAttempt open = beginOpen(writer);
        ReadOnlyManagedLedger readOnly = open.result.get(5, TimeUnit.SECONDS);
        TrackedLedger tracked = trackingFactory.createdLedgers.get(0);
        writer.close();
        trackingFactory.setShutdownLedgerCloseTimeoutMillis(50);
        int handleCount = openedHandles.size();
        CompletableFuture<Void> mappingStarted = new CompletableFuture<>();
        CompletableFuture<Void> allowMappingToProceed = new CompletableFuture<>();
        CompletableFuture<Void> closeStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            mappingStarted.complete(null);
            // The cache owns the promise before this backend open is allowed to return.
            allowMappingToProceed.get(20, TimeUnit.SECONDS);
            return trackOpens((OpenBuilder) invocation.callRealMethod());
        }).when(bkc).newOpenLedgerOp();
        doAnswer(invocation -> {
            closeStarted.complete(null);
            return invocation.callRealMethod();
        }).when(tracked.ledger()).asyncClose();
        AtomicInteger readCallbacks = new AtomicInteger();
        CompletableFuture<Void> reading = new CompletableFuture<>();
        ExecutorService tasks = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("read-only-shutdown-blocked-open"));
        try {
            Future<?> readCall = tasks.submit(() -> readOnly.asyncReadEntry(position,
                    new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            readCallbacks.incrementAndGet();
                            entry.release();
                            reading.complete(null);
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                            readCallbacks.incrementAndGet();
                            reading.completeExceptionally(exception);
                        }
                    }, null));
            mappingStarted.get(5, TimeUnit.SECONDS);

            Future<CompletableFuture<Void>> shutdownCall = tasks.submit(factory::shutdownAsync);
            CompletableFuture<Void> shutdown = shutdownCall.get(5, TimeUnit.SECONDS);
            closeStarted.get(5, TimeUnit.SECONDS);
            shutdown.get(5, TimeUnit.SECONDS);

            // Logical shutdown completes while the backend open is still blocked; its late handle remains owned.
            assertThat(allowMappingToProceed).isNotDone();
            assertThat(tracked.closeCallbacks()).hasValue(1);
            assertThat(readCallbacks).hasValue(0);
            allowMappingToProceed.complete(null);
            readCall.get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> reading.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(openedHandles.size()).isGreaterThan(handleCount);
                verify(openedHandles.get(handleCount)).closeAsync();
                assertThat(tracked.closeCallbacks()).hasValue(1);
            });
            assertThat(readCallbacks).hasValue(1);
            assertThat(open.successes).hasValue(1);
            assertThat(open.failures).hasValue(0);
            assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
        } finally {
            allowMappingToProceed.complete(null);
            tasks.shutdownNow();
            assertThat(tasks.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void testOpenAfterFactoryShutdownFailsOnceWithoutBookKeeperOpen() throws Exception {
        ManagedLedgerImpl writer = writer("read-only-open-after-shutdown");
        factory.shutdownAsync().get(5, TimeUnit.SECONDS);
        clearInvocations(bkc);

        OpenAttempt open = beginOpen(writer);

        assertThatThrownBy(() -> open.result.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerFactoryClosedException.class);
        assertThat(open.successes).hasValue(0);
        assertThat(open.failures).hasValue(1);
        assertThat(trackingFactory.createdLedgers).isEmpty();
        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
        verify(bkc, never()).newOpenLedgerOp();
    }

    @Test
    public void testInitializationFailureClosesAndDeregistersLedger() throws Exception {
        ManagedLedgerImpl writer = writer("read-only-initialization-failure");
        trackingFactory.initializationGate = new CompletableFuture<>();
        OpenAttempt open = beginOpen(writer);
        trackingFactory.initialized.get(5, TimeUnit.SECONDS);
        TrackedLedger tracked = trackingFactory.createdLedgers.get(0);
        ReadHandle handle = readAndCaptureHandle(tracked.ledger(), writer.getLastConfirmedEntry());

        trackingFactory.initializationGate.completeExceptionally(new IllegalStateException("initialization failed"));

        assertThatThrownBy(() -> open.result.get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(IllegalStateException.class);
        assertThat(open.successes).hasValue(0);
        assertThat(open.failures).hasValue(1);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
            assertThat(tracked.closeCallbacks()).hasValue(1);
            verify(handle).closeAsync();
        });
    }

    @Test(dataProvider = "shutdownTimeouts")
    public void testShutdownWhileAcquiringBookKeeperFailsOpenWithoutCreatingLedger(boolean afterTimeout)
            throws Exception {
        ManagedLedgerImpl writer = writer("read-only-acquiring-bookkeeper");
        bookKeeperAccess = new CompletableFuture<>();
        if (afterTimeout) {
            trackingFactory.setShutdownLedgerCloseTimeoutMillis(50);
        }
        clearInvocations(bkc);
        OpenAttempt open = beginOpen(writer);
        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isEqualTo(1);
        assertThat(open.result).isNotDone();

        CompletableFuture<Void> shutdown = factory.shutdownAsync();
        assertThatThrownBy(() -> open.result.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerFactoryClosedException.class);
        if (afterTimeout) {
            shutdown.get(5, TimeUnit.SECONDS);
        } else {
            assertThat(shutdown).isNotDone();
        }
        bookKeeperAccess.complete(bkc);
        shutdown.get(5, TimeUnit.SECONDS);

        assertThat(open.successes).hasValue(0);
        assertThat(open.failures).hasValue(1);
        assertThat(trackingFactory.createdLedgers).isEmpty();
        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
        verify(bkc, never()).newOpenLedgerOp();
    }

    @DataProvider(name = "shutdownTimeouts")
    public Object[][] shutdownTimeouts() {
        return new Object[][] {{false}, {true}};
    }

    @Test
    public void testShutdownClosesLedgerBeforeInitializationFinishes() throws Exception {
        ManagedLedgerImpl writer = writer("read-only-late-initialization");
        trackingFactory.initializationGate = new CompletableFuture<>();
        trackingFactory.setShutdownLedgerCloseTimeoutMillis(50);
        OpenAttempt open = beginOpen(writer);
        trackingFactory.initialized.get(5, TimeUnit.SECONDS);
        TrackedLedger tracked = trackingFactory.createdLedgers.get(0);
        Position position = writer.getLastConfirmedEntry();
        ReadHandle handle = readAndCaptureHandle(tracked.ledger(), position);

        factory.shutdownAsync().get(5, TimeUnit.SECONDS);

        assertThatThrownBy(() -> open.result.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerFactoryClosedException.class);
        assertThat(trackingFactory.initializationGate).isNotDone();
        assertThat(tracked.closeCallbacks()).hasValue(1);
        verify(handle).closeAsync();
        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();

        trackingFactory.initializationGate.complete(null);

        assertThatThrownBy(() -> read(tracked.ledger(), position).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
        assertThat(tracked.closeCallbacks()).hasValue(1);
        verify(handle).closeAsync();
        assertThat(open.successes).hasValue(0);
        assertThat(open.failures).hasValue(1);
    }

    @Test
    public void testShutdownReleasesTemporaryHandleWhileLastAddConfirmedReadIsPending() throws Exception {
        ManagedLedgerImpl writer = writer("read-only-pending-last-add-confirmed");
        Position position = writer.getLastConfirmedEntry();
        int baseline = openedHandles.size();
        lastAddConfirmedGate = new CompletableFuture<>();
        OpenAttempt open = beginOpen(writer);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> openedHandles.size() > baseline);
        ReadHandle temporaryHandle = openedHandles.get(baseline);
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(temporaryHandle).readLastAddConfirmedAsync());
        assertThat(open.result).isNotDone();
        verify(temporaryHandle, never()).closeAsync();

        factory.shutdownAsync().get(5, TimeUnit.SECONDS);

        assertThat(lastAddConfirmedGate).isNotDone();
        verify(temporaryHandle).closeAsync();
        assertThatThrownBy(() -> open.result.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerFactoryClosedException.class);
        assertThat(trackingFactory.createdLedgers.get(0).closeCallbacks()).hasValue(1);

        lastAddConfirmedGate.complete(position.getEntryId());

        ReadOnlyManagedLedger closedLedger = trackingFactory.createdLedgers.get(0).ledger();
        assertThatThrownBy(() -> read(closedLedger, position).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerAlreadyClosedException.class);
        assertThat(trackingFactory.createdLedgers.get(0).closeCallbacks()).hasValue(1);
        verify(temporaryHandle).closeAsync();
        assertThat(open.successes).hasValue(0);
        assertThat(open.failures).hasValue(1);
        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
    }

    @Test
    public void testFactoryCursorOwnsItsLedgerWhileBorrowedCursorDoesNot() throws Exception {
        ManagedLedgerImpl writer = writer("read-only-cursor-ownership");
        ReadOnlyCursor owned = factory.openReadOnlyCursor(writer.getName(), PositionFactory.EARLIEST,
                writer.getConfig());
        // Initialization's temporary handle is released separately from the cursor's cached read handle.
        int baseline = openedHandles.size();
        List<Entry> ownedEntries = owned.readEntries(1);
        ownedEntries.forEach(Entry::release);
        assertThat(openedHandles.size()).isGreaterThan(baseline);
        ReadHandle ownedHandle = openedHandles.get(baseline);
        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isEqualTo(1);

        owned.close();
        owned.close();

        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(ownedHandle).closeAsync());
        assertThat(trackingFactory.createdLedgers.get(0).closeCallbacks()).hasValue(1);

        ReadOnlyManagedLedger ledger = open(writer);
        ReadOnlyCursor borrowed = ledger.createReadOnlyCursor(PositionFactory.EARLIEST);
        baseline = openedHandles.size();
        List<Entry> borrowedEntries = borrowed.readEntries(1);
        borrowedEntries.forEach(Entry::release);
        ReadHandle borrowedHandle = openedHandles.get(baseline);

        borrowed.close();

        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isEqualTo(1);
        verify(borrowedHandle, never()).closeAsync();
        read(ledger, writer.getLastConfirmedEntry()).get(5, TimeUnit.SECONDS).release();
        ledger.asyncClose().get(5, TimeUnit.SECONDS);
        assertThat(trackingFactory.getOpenReadOnlyManagedLedgerCount()).isZero();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(borrowedHandle).closeAsync());
        writer.addEntry(new byte[] {2});
    }
}

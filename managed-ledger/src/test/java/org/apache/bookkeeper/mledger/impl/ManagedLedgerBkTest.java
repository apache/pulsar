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

import static org.apache.pulsar.common.util.PortManager.releaseLockedPort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.util.ThrowableToStringUtil;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class ManagedLedgerBkTest extends BookKeeperClusterTestCase {

    private final ObjectMapper jackson = new ObjectMapper();

    public ManagedLedgerBkTest() {
        super(2);
    }

    @Test(timeOut = 30000)
    public void testInitializationMetadataConflictClosesUnpublishedWriteHandle() throws Exception {
        verifyInitializationMetadataFailureCleanup(true);
    }

    @Test(timeOut = 30000)
    public void testInitializationMetadataWriteTimeoutKeepsPublishedLedger() throws Exception {
        verifyInitializationMetadataFailureCleanup(false);
    }

    private void verifyInitializationMetadataFailureCleanup(boolean badVersion) throws Exception {
        String name = "initialization-failure-" + testName;
        String path = "/managed-ledgers/" + name;
        @Cleanup
        BookKeeperTestClient client = new BookKeeperTestClient(
                new ClientConfiguration(baseClientConf).setAddEntryQuorumTimeout(10)
                        .setNumIOThreads(1).setNumWorkerThreads(1));
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl initialFactory = new ManagedLedgerFactoryImpl(metadataStore, client);
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        initialFactory.open(name, config).close();
        GetResult original = metadataStore.get(path).get(5, TimeUnit.SECONDS).orElseThrow();
        AtomicBoolean changeVersion = new AtomicBoolean(true);
        AtomicBoolean loseWriteResponse = new AtomicBoolean(true);
        // Perform a real competing metadata update after the read, so the initialization's CAS loses.
        FaultInjectionMetadataStore conflictingStore = new FaultInjectionMetadataStore(metadataStore) {
            @Override
            public CompletableFuture<Optional<GetResult>> get(String requestedPath) {
                return super.get(requestedPath).thenCompose(result -> {
                    if (badVersion && requestedPath.equals(path) && changeVersion.compareAndSet(true, false)) {
                        GetResult read = result.orElseThrow();
                        return metadataStore.put(path, read.getValue(), Optional.of(read.getStat().getVersion()))
                                .thenApply(__ -> result);
                    }
                    return CompletableFuture.completedFuture(result);
                });
            }

            @Override
            public CompletableFuture<Stat> put(String requestedPath, byte[] value, Optional<Long> expectedVersion) {
                return super.put(requestedPath, value, expectedVersion).thenCompose(stat -> {
                    if (!badVersion && requestedPath.equals(path) && loseWriteResponse.compareAndSet(true, false)) {
                        // Commit the real ledger-list update, then lose its response. A timeout cannot establish
                        // that the write failed, so cleanup must retain this now-published ledger.
                        return CompletableFuture.failedFuture(new MetadataStoreException(
                                new TimeoutException("Lost metadata write response")));
                    }
                    return CompletableFuture.completedFuture(stat);
                });
            }
        };
        AtomicReference<ManagedLedgerImpl> failedInstance = new AtomicReference<>();
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(conflictingStore, client) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String ledgerName,
                    ManagedLedgerConfig ledgerConfig, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                ManagedLedgerImpl ledger = super.createManagedLedger(bk, store, ledgerName, ledgerConfig,
                        ownershipChecker);
                failedInstance.set(ledger);
                return ledger;
            }
        };
        try {
            assertThatThrownBy(() -> openLedgerAsync(factory, name, config).get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(badVersion ? BadVersionException.class : TimeoutException.class);
            ManagedLedgerImpl failedLedger = failedInstance.get();
            assertThat(failedLedger.getState()).isEqualTo(
                    badVersion ? ManagedLedgerImpl.State.Fenced : ManagedLedgerImpl.State.Closed);
            assertThat(factory.ledgers).doesNotContainKey(name);
            LedgerHandle unpublishedHandle = failedLedger.currentLedger;
            assertThat(unpublishedHandle).isNotNull();
            GetResult winningMetadata = metadataStore.get(path).get(5, TimeUnit.SECONDS).orElseThrow();
            assertThat(winningMetadata.getStat().getVersion()).isEqualTo(original.getStat().getVersion() + 1);
            if (badVersion) {
                assertThat(winningMetadata.getValue()).isEqualTo(original.getValue());
            } else {
                MLDataFormats.ManagedLedgerInfo persisted =
                        MLDataFormats.ManagedLedgerInfo.parseFrom(winningMetadata.getValue());
                assertThat(persisted.getLedgerInfoList().stream()
                        .anyMatch(info -> info.getLedgerId() == unpublishedHandle.getId())).isTrue();
            }
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(client.hasScheduledAddTimeout(unpublishedHandle))
                        .as("the failed initialization must release its periodic add-timeout task").isFalse();
                assertThat(unpublishedHandle.isClosed()).isTrue();
            });
            if (badVersion) {
                Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThatThrownBy(() -> client.getLedgerMetadata(unpublishedHandle.getId())
                                .get(5, TimeUnit.SECONDS))
                                .as("an initialization that loses the metadata CAS must delete its unpublished ledger")
                                .hasRootCauseInstanceOf(BKNoSuchLedgerExistsOnMetadataServerException.class));
            } else {
                assertThat(client.getLedgerMetadata(unpublishedHandle.getId()).get(5, TimeUnit.SECONDS).isClosed())
                        .as("a ledger published before a lost metadata response must remain in BookKeeper").isTrue();
            }
            ManagedLedger retry = factory.open(name, config);
            retry.addEntry("after-conflict".getBytes(StandardCharsets.UTF_8));
            retry.close();
        } finally {
            ManagedLedgerImpl failedLedger = failedInstance.get();
            if (failedLedger != null && failedLedger.currentLedger != null) {
                failedLedger.currentLedger.closeAsync().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test(timeOut = 30000)
    public void testOpenTimesOutDuringCursorPositionUpdate() throws Exception {
        verifyAbandonedOpenDuringCursorPositionUpdate(false, false);
    }

    @Test(timeOut = 30000)
    public void testShutdownDuringCursorPositionUpdateClosesLedger() throws Exception {
        verifyAbandonedOpenDuringCursorPositionUpdate(true, false);
    }

    @Test(timeOut = 30000)
    public void testShutdownWaitsForAbandonedInitializationClose() throws Exception {
        verifyAbandonedOpenDuringCursorPositionUpdate(true, true);
    }

    private void verifyAbandonedOpenDuringCursorPositionUpdate(boolean shutdown, boolean raceAbandonedClose)
            throws Exception {
        String name = "cursor-update-" + testName;
        @Cleanup
        BookKeeperTestClient client = new BookKeeperTestClient(
                new ClientConfiguration(baseClientConf).setAddEntryQuorumTimeout(10)
                        .setNumIOThreads(1).setNumWorkerThreads(1));
        AtomicBoolean gateNextUpdate = new AtomicBoolean();
        CompletableFuture<Void> updateStarted = new CompletableFuture<>();
        CompletableFuture<Void> updateGate = new CompletableFuture<>();
        AtomicBoolean delayClose = new AtomicBoolean();
        AtomicInteger closeCalls = new AtomicInteger();
        CompletableFuture<Void> closeReady = new CompletableFuture<>();
        CompletableFuture<Void> closeGate = new CompletableFuture<>();
        List<ManagedLedgerImpl> instances = new CopyOnWriteArrayList<>();
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setNumManagedLedgerSchedulerThreads(1);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, client, factoryConfig) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String ledgerName,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                ManagedLedgerImpl ledger = new ManagedLedgerImpl(this, bk, store, config, scheduledExecutor,
                        ledgerName, ownershipChecker) {
                    @Override
                    void abandonInitialization() {
                        super.abandonInitialization();
                        if (raceAbandonedClose) {
                            // Shutdown has removed the pending entry, but hasn't failed its open future yet.
                            // Complete the cursor update now so abandonment starts closing the instance first.
                            updateGate.complete(null);
                            try {
                                closeReady.get(5, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new AssertionError("The abandoned initialization did not close", e);
                            }
                        }
                    }

                    @Override
                    public void asyncClose(CloseCallback callback, Object ctx) {
                        if (!delayClose.get() || closeCalls.incrementAndGet() != 1) {
                            super.asyncClose(callback, ctx);
                            return;
                        }
                        // Run the real BookKeeper and cursor closes, then hold their completion delivery.
                        // A second asyncClose must retain its normal behavior, exposing the premature success.
                        super.asyncClose(new CloseCallback() {
                            @Override
                            public void closeComplete(Object context) {
                                closeReady.complete(null);
                                closeGate.thenRun(() -> callback.closeComplete(context));
                            }

                            @Override
                            public void closeFailed(ManagedLedgerException exception, Object context) {
                                closeReady.completeExceptionally(exception);
                                callback.closeFailed(exception, context);
                            }
                        }, ctx);
                    }

                    @Override
                    protected ManagedCursorImpl createCursor(BookKeeper cursorBk, String cursorName) {
                        return new ManagedCursorImpl(cursorBk, this, cursorName) {
                            @Override
                            public void asyncMarkDelete(Position position, Map<String, Long> properties,
                                                        MarkDeleteCallback callback, Object ctx) {
                                if (!gateNextUpdate.compareAndSet(true, false)) {
                                    super.asyncMarkDelete(position, properties, callback, ctx);
                                    return;
                                }
                                // Perform the real mark-delete and persistence, then hold delivery of its callback.
                                // It can be released even after the factory scheduler has stopped.
                                super.asyncMarkDelete(position, properties, new MarkDeleteCallback() {
                                    @Override
                                    public void markDeleteComplete(Object context) {
                                        updateStarted.complete(null);
                                        updateGate.thenRun(() -> callback.markDeleteComplete(context));
                                    }

                                    @Override
                                    public void markDeleteFailed(ManagedLedgerException exception, Object context) {
                                        updateStarted.completeExceptionally(exception);
                                        callback.markDeleteFailed(exception, context);
                                    }
                                }, ctx);
                            }
                        };
                    }
                };
                instances.add(ledger);
                return ledger;
            }
        };
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setLazyCursorRecovery(false).setMetadataOperationsTimeoutSeconds(1)
                .setRetentionSizeInMB(-1).setRetentionTime(-1, TimeUnit.MILLISECONDS);
        try {
            ManagedLedgerImpl initial = (ManagedLedgerImpl) factory.open(name, config);
            ManagedCursor cursor = initial.openCursor("c1");
            cursor.markDelete(initial.addEntry("entry".getBytes(StandardCharsets.UTF_8)));
            initial.close();
            instances.clear();

            // Reopening creates the next data ledger. The recovered cursor must move from the consumed ledger
            // to the new ledger's -1 position, persisting that position before the factory completes the open.
            gateNextUpdate.set(true);
            CompletableFuture<ManagedLedger> pendingOpen = openLedgerAsync(factory, name, config);
            updateStarted.get(10, TimeUnit.SECONDS);
            long updatePendingSince = System.nanoTime();
            ManagedLedgerImpl pendingLedger = instances.get(0);
            LedgerHandle writeHandle = pendingLedger.currentLedger;
            assertThat(pendingLedger.getCursors().iterator().next().getMarkDeletedPosition())
                    .isEqualTo(PositionFactory.create(writeHandle.getId(), -1));
            assertThat(pendingOpen).isNotDone();
            assertThat(client.hasScheduledAddTimeout(writeHandle)).isTrue();

            if (shutdown) {
                factory.setShutdownLedgerCloseTimeoutMillis(5000);
                delayClose.set(raceAbandonedClose);
                CompletableFuture<Void> shutdownFuture = factory.shutdownAsync();
                if (raceAbandonedClose) {
                    // One scheduler thread makes this marker follow the queued shutdown close callback.
                    // On the broken implementation, shutdown can stop the scheduler before it accepts the marker.
                    CompletableFuture<Void> shutdownCallbackProcessed = new CompletableFuture<>();
                    try {
                        factory.getScheduledExecutor().execute(() -> shutdownCallbackProcessed.complete(null));
                    } catch (RejectedExecutionException e) {
                        shutdownCallbackProcessed.complete(null);
                    }
                    CompletableFuture.anyOf(shutdownCallbackProcessed, shutdownFuture).get(5, TimeUnit.SECONDS);
                    assertThat(closeCalls.get()).as("shutdown must share the original close").isEqualTo(1);
                    assertThat(shutdownFuture).as("the first close callback is still pending").isNotDone();
                    assertThat(factory.getScheduledExecutor().isShutdown()).isFalse();
                    closeGate.complete(null);
                }
                shutdownFuture.get(10, TimeUnit.SECONDS);
                if (raceAbandonedClose) {
                    assertThatThrownBy(() -> pendingOpen.get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(ManagedLedgerException.class)
                            .hasMessageContaining("Managed ledger initialization was abandoned");
                } else {
                    assertThatThrownBy(() -> pendingOpen.get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFactoryClosedException.class);
                }
                assertThat(pendingLedger.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
                assertThat(((ManagedCursorImpl) pendingLedger.getCursors().iterator().next()).getState())
                        .isEqualTo("Closed");
            } else {
                Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                        System.nanoTime() - updatePendingSince > TimeUnit.SECONDS.toNanos(1));
                ManagedLedgerImpl replacement = (ManagedLedgerImpl) openLedgerAsync(factory, name, config)
                        .get(5, TimeUnit.SECONDS);
                assertThatThrownBy(() -> pendingOpen.get(5, TimeUnit.SECONDS))
                        .hasMessageContaining("initialization timed out");
                assertThat(replacement).isNotSameAs(pendingLedger);
                assertThat(factory.ledgers.get(name).join()).isSameAs(replacement);
            }

            updateGate.complete(null);
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(pendingLedger.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
                // A retry may already have deleted the empty ledger. isClosed() reports distributed metadata,
                // whereas cancellation of this task checks release of the original handle's local resources.
                assertThat(client.hasScheduledAddTimeout(writeHandle)).isFalse();
            });
            if (!shutdown) {
                ManagedLedgerImpl replacement = factory.ledgers.get(name).join();
                assertThat(factory.getEntryCacheManager().getEntryCache(replacement)).isSameAs(replacement.entryCache);
                Position added = replacement.addEntry("after-retry".getBytes(StandardCharsets.UTF_8));
                replacement.openCursor("c1").markDelete(added);
            }
        } finally {
            closeGate.complete(null);
            updateGate.complete(null);
            // Also close an orphan on the unpatched baseline before the shared BookKeeper client is stopped.
            for (ManagedLedgerImpl ledger : instances) {
                if (shutdown) {
                    if (ledger.currentLedger != null) {
                        ledger.currentLedger.closeAsync().get(5, TimeUnit.SECONDS);
                    }
                } else if (!ledger.getState().isFenced()) {
                    ledger.close();
                }
            }
            try {
                factory.shutdownAsync().get(5, TimeUnit.SECONDS);
            } catch (ManagedLedgerException.ManagedLedgerFactoryClosedException alreadyClosed) {
                // The shutdown case already closed the factory.
            }
        }
    }

    private static CompletableFuture<ManagedLedger> openLedgerAsync(ManagedLedgerFactoryImpl factory, String name,
                                                                   ManagedLedgerConfig config) {
        CompletableFuture<ManagedLedger> result = new CompletableFuture<>();
        factory.asyncOpen(name, config, new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null, null);
        return result;
    }


    @Test
    public void testEstimatedUnackedSizeDuringRolloverWithPendingAdd() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setRetentionSizeInMB(-1).setRetentionTime(-1, TimeUnit.MILLISECONDS);
        config.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("pending-add-" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");
        byte[] data = "entry".getBytes(StandardCharsets.UTF_8);
        ledger.addEntry(data);
        Position lastPosition = ledger.addEntry(data);
        cursor.markDelete(lastPosition);
        config.setMaxEntriesPerLedger(2);

        CompletableFuture<Position> pendingAdd = new CompletableFuture<>();
        CountDownLatch resumeBookie = new CountDownLatch(1);
        CountDownLatch bookieSuspended = new CountDownLatch(1);
        try {
            // Both bookies must acknowledge the write. Hold one so the first add to the new ledger stays pending.
            sleepBookie(getBookie(0), resumeBookie, bookieSuspended);
            assertThat(bookieSuspended.await(10, TimeUnit.SECONDS)).isTrue();
            ledger.rollCurrentLedgerIfFull();
            ledger.asyncAddEntry(data, new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    pendingAdd.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    pendingAdd.completeExceptionally(exception);
                }
            }, null);

            // Rollover itself moves the caught-up cursor to the new ledger's -1 sentinel.
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(cursor.getMarkDeletedPosition().getLedgerId()).isGreaterThan(lastPosition.getLedgerId());
                assertThat(cursor.getMarkDeletedPosition().getEntryId()).isEqualTo(-1);
                assertThat(ledger.getCurrentLedgerEntries()).isEqualTo(1);
            });
            assertThat(cursor.getMarkDeletedPosition().getLedgerId()).isEqualTo(ledger.currentLedger.getId());
            assertThat(ledger.getLastPosition()).isEqualTo(lastPosition);
            // Keep the old ledger so the removed-ledger shortcut cannot mask the regression.
            assertThat(ledger.ledgerExists(lastPosition.getLedgerId())).isTrue();
            assertThat(pendingAdd).isNotDone();
            assertThat(cursor.getEstimatedSizeSinceMarkDeletePosition()).isZero();
        } finally {
            resumeBookie.countDown();
        }

        Position newPosition = pendingAdd.get(10, TimeUnit.SECONDS);
        assertThat(newPosition.getLedgerId()).isGreaterThan(lastPosition.getLedgerId());
        assertThat(newPosition.getEntryId()).isZero();
        assertThat(cursor.getEstimatedSizeSinceMarkDeletePosition()).isEqualTo(data.length);
        List<Entry> entries = cursor.readEntries(1);
        try {
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).getPosition()).isEqualTo(newPosition);
            assertThat(entries.get(0).getData()).isEqualTo(data);
        } finally {
            entries.forEach(Entry::release);
        }
        cursor.markDelete(newPosition);
        assertThat(cursor.getEstimatedSizeSinceMarkDeletePosition()).isZero();
    }

    @Test
    public void testEstimatedUnackedSizeRejectsUnconfirmedEntryInEmptyLedger() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setRetentionSizeInMB(-1).setRetentionTime(-1, TimeUnit.MILLISECONDS);
        config.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("invalid-position-" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");
        Position lastPosition = ledger.addEntry("entry".getBytes(StandardCharsets.UTF_8));
        cursor.markDelete(lastPosition);
        config.setMaxEntriesPerLedger(1);
        ledger.rollCurrentLedgerIfFull();

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(cursor.getMarkDeletedPosition().getLedgerId()).isGreaterThan(lastPosition.getLedgerId());
            assertThat(cursor.getMarkDeletedPosition().getEntryId()).isEqualTo(-1);
        });
        assertThat(cursor.getEstimatedSizeSinceMarkDeletePosition()).isZero();

        // asyncMarkDelete currently accepts an ahead-of-LAC entry in the next ledger after consuming the old one.
        // The estimator must not mistake that entry for the valid -1 sentinel just because the ledger is empty.
        Position invalidPosition = PositionFactory.create(cursor.getMarkDeletedPosition().getLedgerId(), 0);
        cursor.markDelete(invalidPosition);
        assertThat(cursor.getMarkDeletedPosition()).isEqualTo(invalidPosition);
        assertThat(ledger.getCurrentLedgerEntries()).isZero();
        assertThat(ledger.getLastPosition()).isEqualTo(lastPosition);
        assertThat(ledger.ledgerExists(lastPosition.getLedgerId())).isTrue();
        assertThatThrownBy(cursor::getEstimatedSizeSinceMarkDeletePosition)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is ahead of the last position");
    }

    @Test(timeOut = 30000)
    public void testReadOnlyDirectReadLastPublishedEntryBeyondCursorFrontier() throws Exception {
        assertThat(baseClientConf.getExplictLacInterval()).isZero();
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(0);
        @Cleanup("shutdown")
        ManagedLedgerFactory writerFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        @Cleanup("shutdown")
        ManagedLedgerFactory readerFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2);
        String name = "read-only-published-entry-" + UUID.randomUUID();
        ManagedLedger writer = writerFactory.open(name, config);
        writer.addEntry("first".getBytes(StandardCharsets.UTF_8));
        Position penultimate = writer.addEntry("penultimate".getBytes(StandardCharsets.UTF_8));
        Position published = writer.addEntry("published".getBytes(StandardCharsets.UTF_8));

        CompletableFuture<ReadOnlyManagedLedger> opened = new CompletableFuture<>();
        readerFactory.asyncOpenReadOnlyManagedLedger(name, new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger ledger, Object ctx) {
                opened.complete(ledger);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException error, Object ctx) {
                opened.completeExceptionally(error);
            }
        }, config, null);
        ReadOnlyManagedLedger reader = opened.get(10, TimeUnit.SECONDS);
        @Cleanup
        ReadOnlyCursor cursor = reader.createReadOnlyCursor(PositionFactory.EARLIEST);
        long confirmedEntries = reader.getNumberOfEntries();
        assertThat(confirmedEntries).isEqualTo(2);
        // Cursor creation initializes only the confirmed frontier; it does not read or populate the entry cache.
        // No later write publishes a piggyback LAC for the final successful receipt.
        for (Position position : List.of(penultimate, published)) {
            CompletableFuture<Entry> read = new CompletableFuture<>();
            reader.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(Entry entry, Object ctx) {
                    read.complete(entry);
                }

                @Override
                public void readEntryFailed(ManagedLedgerException error, Object ctx) {
                    read.completeExceptionally(error);
                }
            }, null);
            Entry entry = read.get(10, TimeUnit.SECONDS);
            try {
                assertThat(entry.getPosition()).isEqualTo(position);
                String expected = position.equals(published) ? "published" : "penultimate";
                assertThat(entry.getData()).isEqualTo(expected.getBytes(StandardCharsets.UTF_8));
            } finally {
                entry.release();
            }
        }
        assertThat(reader.getNumberOfEntries()).isEqualTo(confirmedEntries);
        assertThat(cursor.getNumberOfEntries()).isEqualTo(confirmedEntries);
        List<Entry> confirmed = cursor.readEntries(10);
        try {
            assertThat(confirmed).hasSize(2);
            assertThat(confirmed.get(0).getPosition().getEntryId()).isEqualTo(0);
            assertThat(confirmed.get(1).getPosition()).isEqualTo(penultimate);
        } finally {
            confirmed.forEach(Entry::release);
        }
        assertThat(cursor.hasMoreEntries()).isFalse();
        assertThat(cursor.getNumberOfEntries()).isZero();
        assertThat(reader.getNumberOfEntries()).isEqualTo(confirmedEntries);
    }

    @Test(timeOut = 30000)
    public void testReadOnlyDirectReadWithSharedCacheAfterRollover() throws Exception {
        assertThat(baseClientConf.getExplictLacInterval()).isZero();
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setMaxCacheSize(16 * 1024 * 1024);
        @Cleanup("shutdown")
        ManagedLedgerFactory writerFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl readerFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConfig);
        ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setRetentionTime(1, TimeUnit.HOURS).setRetentionSizeInMB(-1).setMaxEntriesPerLedger(3);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        String name = "read-only-shared-cache-rollover-" + UUID.randomUUID();
        ManagedLedger writer = writerFactory.open(name, config);
        Position first = writer.addEntry("old-first".getBytes(StandardCharsets.UTF_8));
        writer.addEntry("old-second".getBytes(StandardCharsets.UTF_8));

        ReadOnlyManagedLedger firstReader = openReadOnlyForSharedCacheTest(readerFactory, name, config);
        @Cleanup
        ReadOnlyCursor firstCursor = firstReader.createReadOnlyCursor(PositionFactory.EARLIEST);
        // Public cursor/ledger hooks inspect metadata without reflecting into the wrapper or reading any entry.
        ManagedLedgerImpl firstView = (ManagedLedgerImpl) ((ManagedCursor) firstCursor).getManagedLedger();
        Position firstFrontier = firstView.getLastConfirmedEntry();
        long firstCount = firstReader.getNumberOfEntries();
        Position firstReadPosition = firstCursor.getReadPosition();
        assertThat(firstFrontier).isEqualTo(first);
        assertThat(firstCount).isEqualTo(1);
        EntryCache sharedCache = readerFactory.getEntryCacheManager().getEntryCache(firstView);

        writer.addEntry("rollover-edge".getBytes(StandardCharsets.UTF_8));
        Position newFirst = writer.addEntry("new-first".getBytes(StandardCharsets.UTF_8));
        Position published = writer.addEntry("new-last-published".getBytes(StandardCharsets.UTF_8));
        assertThat(published.getLedgerId()).isNotEqualTo(first.getLedgerId());
        assertThat(published.getLedgerId()).isEqualTo(newFirst.getLedgerId());
        assertThat(firstView.getOptionalLedgerInfo(published.getLedgerId())).isEmpty();
        try (ReadHandle handle = bkc.newOpenLedgerOp().withRecovery(false).withLedgerId(published.getLedgerId())
                .withDigestType(config.getDigestType()).withPassword(config.getPassword())
                .execute().get(10, TimeUnit.SECONDS)) {
            assertThat(handle.isClosed()).isFalse();
            assertThat(handle.getLastAddConfirmed()).isEqualTo(newFirst.getEntryId());
            assertThat(handle.getLastAddConfirmed()).isLessThan(published.getEntryId());
        }

        ReadOnlyManagedLedger secondReader = openReadOnlyForSharedCacheTest(readerFactory, name, config);
        @Cleanup
        ReadOnlyCursor secondCursor = secondReader.createReadOnlyCursor(PositionFactory.EARLIEST);
        ManagedLedgerImpl secondView = (ManagedLedgerImpl) ((ManagedCursor) secondCursor).getManagedLedger();
        assertThat(secondView).isNotSameAs(firstView);
        assertThat(secondView.getOptionalLedgerInfo(published.getLedgerId())).isPresent();
        assertThat(readerFactory.getEntryCacheManager().getEntryCache(secondView)).isSameAs(sharedCache);
        Position secondFrontier = secondView.getLastConfirmedEntry();
        long secondCount = secondReader.getNumberOfEntries();
        Position secondReadPosition = secondCursor.getReadPosition();
        assertThat(secondFrontier).isEqualTo(newFirst);
        assertThat(secondCount).isEqualTo(4);

        // No later write publishes a newer piggyback LAC, and no cursor read can populate the cache first.
        CompletableFuture<Entry> read = new CompletableFuture<>();
        secondReader.asyncReadEntry(published, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                read.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException error, Object ctx) {
                read.completeExceptionally(error);
            }
        }, null);
        Entry entry = read.get(10, TimeUnit.SECONDS);
        try {
            assertThat(entry.getPosition()).isEqualTo(published);
            assertThat(entry.getData()).isEqualTo("new-last-published".getBytes(StandardCharsets.UTF_8));
        } finally {
            entry.release();
        }
        assertThat(firstView.getOptionalLedgerInfo(published.getLedgerId())).isEmpty();
        assertThat(firstView.getLastConfirmedEntry()).isEqualTo(firstFrontier);
        assertThat(firstReader.getNumberOfEntries()).isEqualTo(firstCount);
        assertThat(firstCursor.getReadPosition()).isEqualTo(firstReadPosition);
        assertThat(secondView.getLastConfirmedEntry()).isEqualTo(secondFrontier);
        assertThat(secondReader.getNumberOfEntries()).isEqualTo(secondCount);
        assertThat(secondCursor.getReadPosition()).isEqualTo(secondReadPosition);
    }

    private ReadOnlyManagedLedger openReadOnlyForSharedCacheTest(ManagedLedgerFactory factory, String name,
                                                                 ManagedLedgerConfig config) throws Exception {
        CompletableFuture<ReadOnlyManagedLedger> opened = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(name, new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger ledger, Object ctx) {
                opened.complete(ledger);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException error, Object ctx) {
                opened.completeExceptionally(error);
            }
        }, config, null);
        return opened.get(10, TimeUnit.SECONDS);
    }

    @Test
    public void testSimpleRead() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int num = 1;

        for (int i = 0; i < num; i++) {
            String entry = "entry-" + i;
            ledger.addEntry(entry.getBytes());
        }

        List<Entry> entries = cursor.readEntries(num);
        assertEquals(num, entries.size());
        entries.forEach(Entry::release);
    }

    @Test
    public void testBookieFailure() throws Exception {
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("my-cursor");
        ledger.addEntry("entry-0".getBytes());

        killBookie(1);

        // Now we want to simulate that:
        // 1. The write operation fails because we only have 1 bookie available
        // 2. The bk client cannot properly close the ledger (finalizing the number of entries) because ZK is also
        // not available
        // 3. When we re-establish the service one, the ledger recovery will be triggered and the half-committed entry
        // is restored

        // Force to close the ZK client object so that BK will fail to close the ledger
        bkc.getZkHandle().close();

        try {
            ledger.addEntry("entry-1".getBytes());
            fail("should fail");
        } catch (ManagedLedgerException e) {
            // ok
        }

        bkc.close();
        metadataStore.unsetAlwaysFail();

        bkc = new BookKeeperTestClient(baseClientConf);
        int port = startNewBookie();

        // Reconnect a new bk client
        factory.shutdown();

        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory.open("my-ledger" + testName, config);
        cursor = ledger.openCursor("my-cursor");

        // Next add should succeed
        ledger.addEntry("entry-2".getBytes());

        assertEquals(3, cursor.getNumberOfEntriesInBacklog(false));

        List<Entry> entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-0", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);

        // entry-1 which was half-committed will get fully committed during the recovery phase
        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-1", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);

        entries = cursor.readEntries(1);
        assertEquals(1, entries.size());
        assertEquals("entry-2", new String(entries.get(0).getData()));
        entries.forEach(Entry::release);
        factory.shutdown();
        releaseLockedPort(port);
    }

    @Test
    public void verifyConcurrentUsage() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();

        config.setMaxCacheSize(100 * 1024 * 1024);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        final ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-ledger" + testName, conf);

        int numProducers = 1;
        int numConsumers = 1;

        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier barrier = new CyclicBarrier(numProducers + numConsumers + 1);

        List<Future<?>> futures = new ArrayList();

        for (int i = 0; i < numProducers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();

                    while (!done.get()) {
                        ledger.addEntry("entry".getBytes());
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        }

        for (int i = 0; i < numConsumers; i++) {
            final int idx = i;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();

                    ManagedCursor cursor = ledger.openCursor("my-cursor-" + idx);

                    while (!done.get()) {
                        List<Entry> entries = cursor.readEntries(1);
                        if (!entries.isEmpty()) {
                            cursor.markDelete(entries.get(0).getPosition());
                        }

                        entries.forEach(Entry::release);
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        }

        barrier.await();

        Thread.sleep(1 * 1000);

        done.set(true);
        for (Future<?> future : futures) {
            future.get();
        }

        factory.getMbean().refreshStats(1, TimeUnit.SECONDS);

        assertTrue(factory.getMbean().getCacheHitsRate() > 0.0);
        assertEquals(factory.getMbean().getCacheMissesRate(), 0.0);
        assertTrue(factory.getMbean().getCacheHitsThroughput() > 0.0);
        assertEquals(factory.getMbean().getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void verifyAsyncReadEntryUsingCache() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();

        config.setMaxCacheSize(100 * 1024 * 1024);
        config.setCacheEvictionTimeThresholdMillis(10000);
        config.setCacheEvictionIntervalMs(10000);

        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);

        ManagedLedgerConfig conf = new ManagedLedgerConfig();
        conf.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2)
                .setRetentionSizeInMB(-1).setRetentionTime(-1, TimeUnit.MILLISECONDS);
        final ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my-ledger" + testName, conf);

        int numProducers = 5;
        int numConsumers = 10;

        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier barrier = new CyclicBarrier(numProducers + numConsumers + 1);

        List<Future<?>> futures = new ArrayList();
        List<Position> positions = new CopyOnWriteArrayList<>();

        for (int i = 0; i < numProducers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    // wait for all threads to be ready to start at once
                    barrier.await();
                    while (!done.get()) {
                        Position position = ledger.addEntry("entry".getBytes());
                        positions.add(position);
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw FutureUtil.wrapToCompletionException(e);
                }
            }));
        }

        // create a dummy cursor since caching happens only when there are active consumers
        ManagedCursor cursor = ledger.openCursor("dummy");

        for (int i = 0; i < numConsumers; i++) {
            futures.add(executor.submit(() -> {
                try {
                    // wait for all threads to be ready to start at once
                    barrier.await();
                    while (!done.get()) {
                        if (positions.isEmpty()) {
                            Thread.sleep(1);
                            continue;
                        }
                        // Simulate a replay queue read pattern where individual entries are read
                        Position randomPosition = positions.get(ThreadLocalRandom.current().nextInt(positions.size()));
                        // Clone the original instance so that another instance is used in the asyncReadEntry call
                        // This is to test that keys are compared by .equals and not by reference under the covers
                        randomPosition = PositionFactory.create(randomPosition);
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        ledger.asyncReadEntry(randomPosition, new AsyncCallbacks.ReadEntryCallback() {
                            @Override
                            public void readEntryComplete(Entry entry, Object ctx) {
                                entry.release();
                                future.complete(null);
                            }

                            @Override
                            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                                future.completeExceptionally(exception);
                            }
                        }, null);
                        future.get();
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw FutureUtil.wrapToCompletionException(e);
                }
            }));
        }

        // trigger all worker threads at once to continue from the barrier
        barrier.await();

        int testDurationSeconds = 3;
        Thread.sleep(testDurationSeconds * 1000);

        done.set(true);
        for (Future<?> future : futures) {
            future.get();
        }

        factory.getMbean().refreshStats(testDurationSeconds, TimeUnit.SECONDS);

        assertTrue(factory.getMbean().getCacheHitsRate() > 0.0);
        assertEquals(factory.getMbean().getCacheMissesRate(), 0.0);
        assertTrue(factory.getMbean().getCacheHitsThroughput() > 0.0);
        assertEquals(factory.getMbean().getNumberOfCacheEvictions(), 0);
    }

    @Test
    public void testSimple() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig();
        mlConfig.setEnsembleSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1).setWriteQuorumSize(1);
        // set the data ledger size
        mlConfig.setMaxEntriesPerLedger(100);
        // set the metadata ledger size to 1 to kick off many ledger switching cases
        mlConfig.setMetadataMaxEntriesPerLedger(2);
        ManagedLedger ledger = factory.open("ml-simple-ledger", mlConfig);

        ledger.addEntry("test".getBytes());
    }

    @Test
    public void testConcurrentMarkDelete() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig();
        mlConfig.setEnsembleSize(1).setWriteQuorumSize(1)
            .setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
            .setMetadataAckQuorumSize(1);
        // set the data ledger size
        mlConfig.setMaxEntriesPerLedger(100);
        // set the metadata ledger size to 1 to kick off many ledger switching cases
        mlConfig.setMetadataMaxEntriesPerLedger(10);
        ManagedLedger ledger = factory.open("ml-markdelete-ledger", mlConfig);

        final List<Position> addedEntries = new ArrayList();

        int numCursors = 10;
        final CyclicBarrier barrier = new CyclicBarrier(numCursors);

        List<ManagedCursor> cursors = new ArrayList();
        for (int i = 0; i < numCursors; i++) {
            cursors.add(ledger.openCursor(String.format("c%d", i)));
        }

        for (int i = 0; i < 50; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        List<Future<?>> futures = new ArrayList();

        for (ManagedCursor cursor : cursors) {
            futures.add(executor.submit(() -> {
                barrier.await();

                for (Position position : addedEntries) {
                    cursor.markDelete(position);
                }

                return null;
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        // Since in this test we roll-over the cursor ledger every 10 entries acknowledged, the background roll back
        // might still be happening when the futures are completed.
        Thread.sleep(1000);
    }

    @Test
    public void asyncMarkDeleteAndClose() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        ManagedLedgerConfig config = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        List<Position> positions = new ArrayList();

        for (int i = 0; i < 10; i++) {
            Position p = ledger.addEntry("entry".getBytes());
            positions.add(p);
        }

        final CountDownLatch counter = new CountDownLatch(positions.size());
        final AtomicReference<Exception> gotException = new AtomicReference();

        for (Position p : positions) {
            cursor.asyncDelete(p, new DeleteCallback() {
                @Override
                public void deleteComplete(Object ctx) {
                    // Ok
                    counter.countDown();
                }

                @Override
                public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                    gotException.set(exception);
                    counter.countDown();
                }
            }, null);
        }

        counter.await();

        // cleanup.
        closeManagedLedgerWithRetry(ledger);

        // Add information to determine the problem.
        if (gotException.get() != null){
            fail(ThrowableToStringUtil.toString(gotException.get()));
        }
    }

    /**
     * When auto-replication is triggered, if there were no writes on the ML during the grace period, auto-replication
     * will close the ledger an re-replicate it. After that, the next write will get a FencedException. We should
     * recover from this condition by creating a new ledger and retrying the write.
     */
    @Test
    public void ledgerFencedByAutoReplication() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger" + testName, config);
        ManagedCursor c1 = ledger.openCursor("c1");

        Position p1 = ledger.addEntry("entry-1".getBytes());

        // Trigger the closure of the data ledger
        bkc.openLedger(p1.getLedgerId(), BookKeeper.DigestType.CRC32C, new byte[] {});

        ledger.addEntry("entry-2".getBytes());

        assertEquals(2, c1.getNumberOfEntries());
        assertEquals(2, c1.getNumberOfEntriesInBacklog(false));

        Position p3 = ledger.addEntry("entry-3".getBytes());

        // Now entry-2 should have been written before entry-3
        assertEquals(3, c1.getNumberOfEntries());
        assertEquals(3, c1.getNumberOfEntriesInBacklog(false));
        assertTrue(p1.getLedgerId() != p3.getLedgerId());
    }

    /**
     * When another process steals the ML, the old instance should not succeed in any operation.
     */
    @Test
    public void ledgerFencedByFailover() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory1.open("my_test_ledger" + testName, config);
        ledger1.openCursor("c");

        ledger1.addEntry("entry-1".getBytes());

        // Open the ML from another factory
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerImpl ledger2 = (ManagedLedgerImpl) factory2.open("my_test_ledger" + testName, config);
        ManagedCursor c2 = ledger2.openCursor("c");

        try {
            ledger1.addEntry("entry-2".getBytes());
            fail("Should have failed");
        } catch (ManagedLedgerException e) {
            // Ok
        }

        ledger2.addEntry("entry-2".getBytes());

        try {
            ledger1.addEntry("entry-2".getBytes());
            fail("Should have failed");
        } catch (ManagedLedgerException bve) {
            // Ok
        }

        assertEquals(2, c2.getNumberOfEntriesInBacklog(false));
    }

    @Test
    public void testOfflineTopicBacklog() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("property/cluster/namespace/my-ledger", config);
        ManagedCursor cursor = ledger.openCursor("c1");

        int num = 1;

        for (int i = 0; i < num; i++) {
            String entry = "entry-" + i;
            ledger.addEntry(entry.getBytes());
        }

        List<Entry> entries = cursor.readEntries(num);
        assertEquals(num, entries.size());
        entries.forEach(Entry::release);
        ledger.close();

        ManagedLedgerOfflineBacklog offlineTopicBacklog = new ManagedLedgerOfflineBacklog(
                DigestType.CRC32, "".getBytes(StandardCharsets.UTF_8), "", false);
        PersistentOfflineTopicStats offlineTopicStats = offlineTopicBacklog.getEstimatedUnloadedTopicBacklog(
                (ManagedLedgerFactoryImpl) factory, "property/cluster/namespace/my-ledger");
        assertNotNull(offlineTopicStats);
    }

    @Test(timeOut = 20000)
    void testResetCursorAfterRecovery() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig conf = new ManagedLedgerConfig().setMaxEntriesPerLedger(10).setEnsembleSize(1)
                .setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
                .setMetadataAckQuorumSize(1);
        ManagedLedger ledger = factory.open("my_test_move_cursor_ledger", conf);
        ManagedCursor cursor = ledger.openCursor("trc1");
        Position p1 = ledger.addEntry("dummy-entry-1".getBytes());
        Position p2 = ledger.addEntry("dummy-entry-2".getBytes());
        Position p3 = ledger.addEntry("dummy-entry-3".getBytes());
        Position p4 = ledger.addEntry("dummy-entry-4".getBytes());

        cursor.markDelete(p3);

        @Cleanup("shutdown")
        ManagedLedgerFactory factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_test_move_cursor_ledger", conf);
        cursor = ledger.openCursor("trc1");

        assertEquals(cursor.getMarkDeletedPosition(), p3);
        assertEquals(cursor.getReadPosition(), p4);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 1);

        cursor.resetCursor(p2);
        assertEquals(cursor.getMarkDeletedPosition(), p1);
        assertEquals(cursor.getReadPosition(), p2);
        assertEquals(cursor.getNumberOfEntriesInBacklog(false), 3);
    }

    @Test(timeOut = 30000)
    public void managedLedgerClosed() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        ManagedLedgerImpl ledger1 = (ManagedLedgerImpl) factory.open("my_test_ledger" + testName, config);

        int num = 100;

        AtomicReference<ManagedLedgerException> res = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(num);

        for (int i = 0; i < num; i++) {
            ledger1.asyncAddEntry(("entry-" + i).getBytes(), new AddEntryCallback() {

                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    latch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    res.compareAndSet(null, exception);
                    latch.countDown();
                }
            }, null);

            if (i == 1) {
                ledger1.close();
            }
        }

        // Ensures all the callback must have been invoked
        latch.await();
        assertNotNull(res.get());
        assertEquals(res.get().getClass(), ManagedLedgerAlreadyClosedException.class);
    }

    @Test
    public void testChangeCrcType() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(2).setAckQuorumSize(2).setMetadataEnsembleSize(2);
        config.setDigestType(DigestType.CRC32);
        ManagedLedger ledger = factory.open("my_test_ledger" + testName, config);
        ManagedCursor c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-0".getBytes());
        ledger.addEntry("entry-1".getBytes());
        ledger.addEntry("entry-2".getBytes());

        ledger.close();

        config.setDigestType(DigestType.CRC32C);
        ledger = factory.open("my_test_ledger" + testName, config);
        c1 = ledger.openCursor("c1");

        ledger.addEntry("entry-3".getBytes());

        assertEquals(c1.getNumberOfEntries(), 4);
        assertEquals(c1.getNumberOfEntriesInBacklog(false), 4);

        List<Entry> entries = c1.readEntries(4);
        assertEquals(entries.size(), 4);
        for (int i = 0; i < 4; i++) {
            assertEquals(new String(entries.get(i).getData()), "entry-" + i);
        }
    }

    @Test
    public void testPeriodicRollover() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);

        int rolloverTimeForCursorInSeconds = 5;

        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1).setMetadataEnsembleSize(1)
                .setMetadataAckQuorumSize(1)
                .setLedgerRolloverTimeout(rolloverTimeForCursorInSeconds);
        ManagedLedger ledger = factory.open("my-ledger" + testName, config);
        ManagedCursor cursor = ledger.openCursor("c1");

        Position pos = ledger.addEntry("entry-0".getBytes());
        ledger.addEntry("entry-1".getBytes());

        List<Entry> entries = cursor.readEntries(2);
        assertEquals(2, entries.size());
        entries.forEach(Entry::release);

        ManagedCursorImpl cursorImpl = (ManagedCursorImpl) cursor;
        assertEquals(ManagedCursorImpl.State.NoLedger, cursorImpl.state);

        // this creates the ledger
        cursor.delete(pos);

        Awaitility.await().until(() -> cursorImpl.state == ManagedCursorImpl.State.Open);

        Thread.sleep(rolloverTimeForCursorInSeconds * 1000 + 1000);

        long currentLedgerId = cursorImpl.getCursorLedger();
        assertTrue(cursor.periodicRollover());
        Awaitility.await().until(() -> cursorImpl.getCursorLedger() != currentLedgerId);
    }

    @DataProvider(name = "unackedRangesOpenCacheSetEnabledPair")
    public Object[][] unackedRangesOpenCacheSetEnabledPair() {
        return new Object[][]{
            {false, true},
            {true, false},
            {true, true},
            {false, false}
        };
    }

    /**
     * This test validates that cursor serializes and deserializes individual-ack list from the bk-ledger.
     * @throws Exception
     */
    @Test(dataProvider = "unackedRangesOpenCacheSetEnabledPair")
    public void testUnackmessagesAndRecoveryCompatibility(boolean enabled1, boolean enabled2) throws Exception {
        final String mlName = "ml" + UUID.randomUUID().toString().replaceAll("-", "");
        final String cursorName = "c1";
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        final ManagedLedgerConfig config1 = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
                .setMaxUnackedRangesToPersistInMetadataStore(1).setMaxEntriesPerLedger(5).setMetadataAckQuorumSize(1)
                .setUnackedRangesOpenCacheSetEnabled(enabled1);
        final ManagedLedgerConfig config2 = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1).setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1)
                .setMaxUnackedRangesToPersistInMetadataStore(1).setMaxEntriesPerLedger(5).setMetadataAckQuorumSize(1)
                .setUnackedRangesOpenCacheSetEnabled(enabled2);

        ManagedLedger ledger1 = factory.open(mlName, config1);
        ManagedCursorImpl cursor1 = (ManagedCursorImpl) ledger1.openCursor(cursorName);

        int totalEntries = 100;
        for (int i = 0; i < totalEntries; i++) {
            Position p = ledger1.addEntry("entry".getBytes());
            if (i % 2 == 0) {
                cursor1.delete(p);
            }
        }
        log.info("ack ranges: {}", cursor1.getIndividuallyDeletedMessagesSet().size());

        // reopen and recover cursor
        ledger1.close();
        ManagedLedger ledger2 = factory.open(mlName, config2);
        ManagedCursorImpl cursor2 = (ManagedCursorImpl) ledger2.openCursor(cursorName);

        log.info("before: {}", cursor1.getIndividuallyDeletedMessagesSet().asRanges());
        log.info("after : {}", cursor2.getIndividuallyDeletedMessagesSet().asRanges());
        assertEquals(cursor1.getIndividuallyDeletedMessagesSet().asRanges(),
                cursor2.getIndividuallyDeletedMessagesSet().asRanges());
        assertEquals(cursor1.markDeletePosition, cursor2.markDeletePosition);

        ledger2.close();
        factory.shutdown();
    }

    @DataProvider(name = "booleans")
    public Object[][] booleans() {
        return new Object[][] {
                {true},
                {false},
        };
    }

    @Test(dataProvider = "booleans")
    public void testConfigPersistIndividualAckAsLongArray(boolean enable) throws Exception {
        final String mlName = "ml" + UUID.randomUUID().toString().replaceAll("-", "");
        final String cursorName = "c1";
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, factoryConf);
        final ManagedLedgerConfig config = new ManagedLedgerConfig()
                .setEnsembleSize(1).setWriteQuorumSize(1).setAckQuorumSize(1)
                .setMetadataEnsembleSize(1).setMetadataWriteQuorumSize(1).setMetadataAckQuorumSize(1)
                .setMaxUnackedRangesToPersistInMetadataStore(1)
                .setUnackedRangesOpenCacheSetEnabled(true).setPersistIndividualAckAsLongArray(enable);

        ManagedLedger ledger1 = factory.open(mlName, config);
        ManagedCursorImpl cursor1 = (ManagedCursorImpl) ledger1.openCursor(cursorName);

        // Write entries.
        int totalEntries = 100;
        List<Position> entries = new ArrayList<>();
        for (int i = 0; i < totalEntries; i++) {
            Position p = ledger1.addEntry("entry".getBytes());
            entries.add(p);
        }
        // Make ack holes and trigger a mark deletion.
        for (int i = totalEntries - 1; i >= 0; i--) {
            if (i % 2 == 0) {
                cursor1.delete(entries.get(i));
            }
        }
        cursor1.markDelete(entries.get(9));
        Awaitility.await().untilAsserted(() -> {
            assertEquals(cursor1.pendingMarkDeleteOps.size(), 0);
        });

        // Verify: the config affects.
        long cursorLedgerLac = cursor1.cursorLedger.getLastAddConfirmed();
        LedgerEntry ledgerEntry = cursor1.cursorLedger.readEntries(cursorLedgerLac, cursorLedgerLac).nextElement();
        MLDataFormats.PositionInfo positionInfo = MLDataFormats.PositionInfo.parseFrom(ledgerEntry.getEntry());
        if (enable) {
            assertNotEquals(positionInfo.getIndividualDeletedMessageRangesList().size(), 0);
        } else {
            assertEquals(positionInfo.getIndividualDeletedMessageRangesList().size(), 0);
        }

        // cleanup
        ledger1.close();
        factory.shutdown();
    }

    /**
     * Terminating a managed ledger closes the current BookKeeper ledger, and that close errors out the adds which are
     * still in flight. These adds can never succeed, so they must be failed rather than left pending forever: a caller
     * that tracks its in-flight writes (like the broker's PersistentTopic) would otherwise never see them complete.
     */
    @Test
    public void testTerminateFailsInFlightAdds() throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);

        ManagedLedgerConfig config = new ManagedLedgerConfig().setEnsembleSize(1).setWriteQuorumSize(1)
                .setAckQuorumSize(1);
        // A terminated ledger cannot be written to again: use a name that a retry of this test would not reuse
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger" + UUID.randomUUID(), config);
        Position lastPersisted = ledger.addEntry("entry-0".getBytes());

        // Suspend the bookie that stores the current ledger, so that the next add stays in flight
        BookieId bookie = ledger.currentLedger.getLedgerMetadata().getAllEnsembles().firstEntry().getValue().get(0);
        CountDownLatch resumeBookie = new CountDownLatch(1);
        sleepBookie(bookie, resumeBookie);
        try {
            CompletableFuture<ManagedLedgerException> inFlightAddFailure = new CompletableFuture<>();
            ledger.asyncAddEntry("entry-1".getBytes(), new AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    inFlightAddFailure.completeExceptionally(
                            new AssertionError("The in-flight add should not succeed: " + position));
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    inFlightAddFailure.complete(exception);
                }
            }, null);
            Awaitility.await().until(() -> !ledger.pendingAddEntries.isEmpty());

            assertEquals(ledger.terminate(), lastPersisted);

            assertTrue(inFlightAddFailure.get(10, TimeUnit.SECONDS) instanceof ManagedLedgerTerminatedException);
            assertTrue(ledger.pendingAddEntries.isEmpty());
        } finally {
            resumeBookie.countDown();
        }
    }
}

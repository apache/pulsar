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

import static org.apache.bookkeeper.mledger.ManagedLedgerException.getManagedLedgerException;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.AUTOMATIC_OFFLOAD_TRIGGER;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.BoundType;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenReadOnlyCursorCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.CursorInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.MessageRangeInfo;
import org.apache.bookkeeper.mledger.MetadataCompressionConfig;
import org.apache.bookkeeper.mledger.OpenTelemetryManagedLedgerCacheStats;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedgerImplWrapper;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.ManagedLedgerInitializeLedgerCallback;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheManagerImpl;
import org.apache.bookkeeper.mledger.offload.OffloadUtils;
import org.apache.bookkeeper.mledger.proto.KeyValue;
import org.apache.bookkeeper.mledger.proto.LongProperty;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.OffloadContext;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.bookkeeper.mledger.util.Errors;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Runnables;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

@CustomLog
public class ManagedLedgerFactoryImpl implements ManagedLedgerFactory {
    private final MetaStore store;
    private final Runnable cancelSessionListener;
    private final BookkeeperFactoryForCustomEnsemblePlacementPolicy bookkeeperFactory;
    private final boolean isBookkeeperManaged;
    private final ManagedLedgerFactoryConfig config;
    @Getter
    protected final OrderedScheduler scheduledExecutor;
    @Getter
    private final ScheduledExecutorService cacheEvictionExecutor;

    @Getter
    protected final ManagedLedgerFactoryMBeanImpl mbean;

    protected final ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, PendingInitializeManagedLedger> pendingInitializeLedgers =
        new ConcurrentHashMap<>();
    private final EntryCacheManager entryCacheManager;

    private long lastStatTimestamp = System.nanoTime();
    private final ScheduledFuture<?> statsTask;
    private final ScheduledFuture<?> flushCursorsTask;
    private final ReadEntryTimeoutTracker readEntryTimeoutTracker;

    private volatile long cacheEvictionTimeThresholdNanos;
    private final MetadataStore metadataStore;

    private final OpenTelemetryManagedLedgerCacheStats openTelemetryCacheStats;
    @Getter
    private final OpenTelemetryManagedLedgerStats openTelemetryManagedLedgerStats;
    @Getter
    private final OpenTelemetryManagedCursorStats openTelemetryManagedCursorStats;

    //indicate whether shutdown() is called.
    private volatile boolean closed;
    // How long shutdown waits for each managed ledger to close
    private long shutdownLedgerCloseTimeoutMillis =
            TimeUnit.SECONDS.toMillis(ManagedLedgerImpl.AsyncOperationTimeoutSeconds);

    /**
     * Keep a flag to indicate whether we're currently connected to the metadata service.
     */
    @Getter
    private boolean metadataServiceAvailable;
    private volatile boolean metadataSessionLost;
    private final ManagedLedgerConfig defaultManagedLedgerConfig;

    private static class PendingInitializeManagedLedger {

        private final ManagedLedgerImpl ledger;
        private final CompletableFuture<ManagedLedgerImpl> future;
        private final long createTimeMs;
        private volatile boolean ledgerInitialized;
        // State.Closed is set before cleanup finishes, so all factory paths must share one close completion.
        private final AtomicBoolean closeStarted = new AtomicBoolean();
        private final Futures.CloseFuture closeFuture = new Futures.CloseFuture();

        PendingInitializeManagedLedger(ManagedLedgerImpl ledger, CompletableFuture<ManagedLedgerImpl> future) {
            this.ledger = ledger;
            this.future = future;
            this.createTimeMs = System.currentTimeMillis();
        }

    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, ClientConfiguration bkClientConfiguration)
            throws Exception {
        this(metadataStore, bkClientConfiguration, new ManagedLedgerFactoryConfig());
    }

    @SuppressWarnings("deprecation")
    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, ClientConfiguration bkClientConfiguration,
                                    ManagedLedgerFactoryConfig config)
            throws Exception {
        this(metadataStore, new DefaultBkFactory(bkClientConfiguration),
                true /* isBookkeeperManaged */, config, NullStatsLogger.INSTANCE, OpenTelemetry.noop(),
                new ManagedLedgerConfig());
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, BookKeeper bookKeeper)
            throws Exception {
        this(metadataStore, bookKeeper, new ManagedLedgerFactoryConfig());
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, BookKeeper bookKeeper,
                                    ManagedLedgerFactoryConfig config)
            throws Exception {
        this(metadataStore, bookKeeper, config, new ManagedLedgerConfig());
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, BookKeeper bookKeeper,
                                    ManagedLedgerFactoryConfig config, ManagedLedgerConfig defaultManagedLedgerConfig)
            throws Exception {
        this(metadataStore, (policyConfig) -> CompletableFuture.completedFuture(bookKeeper),
                false /* isBookkeeperManaged */, config, NullStatsLogger.INSTANCE, OpenTelemetry.noop(),
                defaultManagedLedgerConfig);
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                    BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                    ManagedLedgerFactoryConfig config)
            throws Exception {
        this(metadataStore, bookKeeperGroupFactory, false /* isBookkeeperManaged */,
                config, NullStatsLogger.INSTANCE, OpenTelemetry.noop(), new ManagedLedgerConfig());
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                    BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                    ManagedLedgerFactoryConfig config, StatsLogger statsLogger,
                                    OpenTelemetry openTelemetry)
            throws Exception {
        this(metadataStore, bookKeeperGroupFactory, false /* isBookkeeperManaged */,
                config, statsLogger, openTelemetry, new ManagedLedgerConfig());
    }

    private ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                     BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                     boolean isBookkeeperManaged,
                                     ManagedLedgerFactoryConfig config,
                                     StatsLogger statsLogger,
                                     OpenTelemetry openTelemetry,
                                     ManagedLedgerConfig defaultManagedLedgerConfig) throws Exception {
        this.defaultManagedLedgerConfig = defaultManagedLedgerConfig;
        MetadataCompressionConfig compressionConfigForManagedLedgerInfo =
                config.getCompressionConfigForManagedLedgerInfo();
        MetadataCompressionConfig compressionConfigForManagedCursorInfo =
                config.getCompressionConfigForManagedCursorInfo();
        scheduledExecutor = OrderedScheduler.newSchedulerBuilder()
                .numThreads(config.getNumManagedLedgerSchedulerThreads())
                .statsLogger(statsLogger)
                .traceTaskExecution(config.isTraceTaskExecution())
                .name("bookkeeper-ml-scheduler")
                .build();
        cacheEvictionExecutor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("bookkeeper-ml-cache-eviction"));
        this.metadataServiceAvailable = true;
        this.bookkeeperFactory = bookKeeperGroupFactory;
        this.isBookkeeperManaged = isBookkeeperManaged;
        this.metadataStore = metadataStore;
        this.store = new MetaStoreImpl(metadataStore, scheduledExecutor,
                compressionConfigForManagedLedgerInfo,
                compressionConfigForManagedCursorInfo);
        this.config = config;
        this.mbean = new ManagedLedgerFactoryMBeanImpl(this);
        this.entryCacheManager = new RangeEntryCacheManagerImpl(this, scheduledExecutor, openTelemetry);
        this.readEntryTimeoutTracker = new ReadEntryTimeoutTracker(scheduledExecutor);
        this.statsTask = scheduledExecutor.scheduleWithFixedDelay(catchingAndLoggingThrowables(this::refreshStats),
                0, config.getStatsPeriodSeconds(), TimeUnit.SECONDS);
        this.flushCursorsTask = scheduledExecutor.scheduleAtFixedRate(catchingAndLoggingThrowables(this::flushCursors),
                config.getCursorPositionFlushSeconds(), config.getCursorPositionFlushSeconds(), TimeUnit.SECONDS);


        this.cacheEvictionTimeThresholdNanos = TimeUnit.MILLISECONDS
                .toNanos(config.getCacheEvictionTimeThresholdMillis());

        long evictionTaskInterval = config.getCacheEvictionIntervalMs();
        cacheEvictionExecutor.scheduleWithFixedDelay(Runnables.catchingAndLoggingThrowables(this::doCacheEviction),
                evictionTaskInterval, evictionTaskInterval, TimeUnit.MILLISECONDS);
        closed = false;

        cancelSessionListener = metadataStore.registerCancellableSessionListener(this::handleMetadataStoreNotification);

        openTelemetryCacheStats = new OpenTelemetryManagedLedgerCacheStats(openTelemetry, this);
        openTelemetryManagedLedgerStats = new OpenTelemetryManagedLedgerStats(openTelemetry, this);
        openTelemetryManagedCursorStats = new OpenTelemetryManagedCursorStats(openTelemetry, this);
    }

    static class DefaultBkFactory implements BookkeeperFactoryForCustomEnsemblePlacementPolicy, AutoCloseable {

        private final BookKeeper bkClient;

        public DefaultBkFactory(ClientConfiguration bkClientConfiguration)
                throws InterruptedException, BKException, IOException {
            bkClient = new BookKeeper(bkClientConfiguration);
        }

        @Override
        public CompletableFuture<BookKeeper> get(EnsemblePlacementPolicyConfig policy) {
            return CompletableFuture.completedFuture(bkClient);
        }

        @Override
        public void close() throws Exception {
            bkClient.close();
        }
    }

    private synchronized void handleMetadataStoreNotification(SessionEvent e) {
        log.info().attr("event", e).log("Received MetadataStore session event");
        metadataServiceAvailable = e.isConnected();
        if (e == SessionEvent.SessionLost) {
            metadataSessionLost = true;
            // On each managed ledger's executor, off the store's event thread: a managed ledger's recheck of a fenced
            // ledger may be waiting for the metadata service
            ledgers.values().forEach(future -> {
                if (future.isDone() && !future.isCompletedExceptionally()) {
                    ManagedLedgerImpl ledger = future.join();
                    try {
                        ledger.getExecutor().execute(ledger::metadataSessionLost);
                    } catch (RejectedExecutionException ex) {
                        // the factory is shutting down
                        log.debug().attr("ledger", ledger.getName()).exceptionMessage(ex)
                                .log("Failed to tell a managed ledger that the metadata session was lost");
                    }
                }
            });
        } else if (e == SessionEvent.SessionReestablished) {
            metadataSessionLost = false;
        }
    }

    /**
     * Whether the metadata store's session was lost and not reestablished yet: the store may have deleted the
     * session's ephemeral nodes, such as its locks, and other clients may have taken over what they protected.
     */
    boolean isMetadataSessionLost() {
        return metadataSessionLost;
    }

    private synchronized void flushCursors() {
        ledgers.values().forEach(mlfuture -> {
            if (mlfuture.isDone() && !mlfuture.isCompletedExceptionally()) {
                ManagedLedgerImpl ml = mlfuture.getNow(null);
                if (ml != null) {
                    ml.getCursors().forEach(c -> ((ManagedCursorImpl) c).flush());
                }
            }
        });
    }

    private synchronized void refreshStats() {
        long now = System.nanoTime();
        long period = now - lastStatTimestamp;

        mbean.refreshStats(period, TimeUnit.NANOSECONDS);
        ledgers.values().forEach(mlfuture -> {
            if (mlfuture.isDone() && !mlfuture.isCompletedExceptionally()) {
                ManagedLedgerImpl ml = mlfuture.getNow(null);
                if (ml != null) {
                    ml.mbean.refreshStats(period, TimeUnit.NANOSECONDS);
                }
            }
        });

        lastStatTimestamp = now;
    }

    @VisibleForTesting
    public synchronized void doCacheEviction() {
        entryCacheManager.doCacheEviction();
    }

    ReadEntryTimeoutTracker getReadEntryTimeoutTracker() {
        return readEntryTimeoutTracker;
    }

    /**
     * Waits for all pending cache evictions based on total cache size or entry TTL to complete.
     * This is for testing purposes only, so that we can ensure all cache evictions are done before proceeding with
     * further operations. Please notice that Managed Ledger level cache eviction is not handled here. There's
     * a similar method {@link ManagedLedgerImpl#waitForPendingCacheEvictions()} that waits for pending cache evictions
     * at the Managed Ledger level.
     */
    @VisibleForTesting
    public void waitForPendingCacheEvictions() {
        try {
            cacheEvictionExecutor.submit(() -> {
                // no-op
            }).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Blocks the pending cache evictions until the returned runnable is called.
     * This is for testing purposes only, so that asynchronous cache evictions can be blocked for consistent
     * test results.
     *
     * @return
     * @throws InterruptedException
     */
    @VisibleForTesting
    public Runnable blockPendingCacheEvictions() throws InterruptedException {
        CountDownLatch blockedLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        cacheEvictionExecutor.execute(() -> {
            blockedLatch.countDown();
            try {
                releaseLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        blockedLatch.await();
        return () -> {
            if (releaseLatch.getCount() == 0) {
                throw new IllegalStateException("Releasing should only be called once");
            }
            releaseLatch.countDown();
            waitForPendingCacheEvictions();
        };
    }

    /**
     * Helper for getting stats.
     *
     * @return
     */
    @Override
    public Map<String, ManagedLedger> getManagedLedgers() {
        // Return a view of already created ledger by filtering futures not yet completed
        return Maps.filterValues(Maps.transformValues(ledgers, future -> future.getNow(null)), Predicates.notNull());
    }

    @Override
    public ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException {
        return open(name, defaultManagedLedgerConfig);
    }

    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedger l = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncOpen(name, config, new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                r.l = ledger;
                latch.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.l;
    }

    @Override
    public void asyncOpen(String name, OpenLedgerCallback callback, Object ctx) {
        asyncOpen(name, defaultManagedLedgerConfig, callback, null, ctx);
    }

    @Override
    public void asyncOpen(final String name, final ManagedLedgerConfig config, final OpenLedgerCallback callback,
            Supplier<CompletableFuture<Boolean>> mlOwnershipChecker, final Object ctx) {
        if (closed) {
            callback.openLedgerFailed(new ManagedLedgerException.ManagedLedgerFactoryClosedException(), ctx);
            return;
        }

        // If the ledger state is bad, remove it from the map.
        CompletableFuture<ManagedLedgerImpl> existingFuture = ledgers.get(name);
        if (existingFuture != null) {
            if (existingFuture.isDone()) {
                try {
                    ManagedLedgerImpl l = existingFuture.get();
                    if (l.getState().isFenced() || l.getState() == State.Closed) {
                        // Managed ledger is in unusable state. Recreate it.
                        log.warn().attr("managedLedger", name)
                                .attr("state", l.getState())
                                .log("Attempted to open ledger in unusable"
                                        + " state. Removing from the map to"
                                        + " recreate it");
                        removeLedgerFuture(name, existingFuture);
                    }
                } catch (Exception e) {
                    // Unable to get the future. Remove it, so that a failed open is retried.
                    log.warn().attr("managedLedger", name).exception(e)
                            .log("Got exception while trying to"
                                    + " retrieve ledger");
                    removeLedgerFuture(name, existingFuture);
                }
            } else {
                PendingInitializeManagedLedger pendingLedger = pendingInitializeLedgers.get(name);
                if (null != pendingLedger) {
                    long pendingMs = System.currentTimeMillis() - pendingLedger.createTimeMs;
                    // Removing the pending entry claims the initialization, so that it can't complete or be timed
                    // out concurrently. The pending entry could belong to a newer open than the future read above,
                    // if the future was already replaced.
                    if (pendingMs > TimeUnit.SECONDS.toMillis(config.getMetadataOperationsTimeoutSeconds())
                            && pendingLedger.future == existingFuture
                            && pendingInitializeLedgers.remove(name, pendingLedger)) {
                        log.warn().attr("managedLedger", name)
                            .attr("pendingMs", pendingMs)
                            .log("Managed ledger has been pending in"
                                    + " initialize state too long,"
                                    + " remove it from cache to retry");
                        pendingLedger.ledger.abandonInitialization();
                        removeLedgerFuture(name, existingFuture);
                        // Fail the opens waiting for the timed out initialization. The managed ledger instance is
                        // closed once its initialization completes or fails.
                        existingFuture.completeExceptionally(new ManagedLedgerException(
                                "Managed ledger initialization timed out after " + pendingMs + " ms"));
                    }
                }
            }
        }

        // Ensure only one managed ledger is created and initialized
        long startTimeOfCreation = System.currentTimeMillis();
        ledgers.computeIfAbsent(name, (mlName) -> {
            // Create the managed ledger
            CompletableFuture<ManagedLedgerImpl> future = new CompletableFuture<>();
            bookkeeperFactory.get(
                            new EnsemblePlacementPolicyConfig(config.getBookKeeperEnsemblePlacementPolicyClassName(),
                                    config.getBookKeeperEnsemblePlacementPolicyProperties()))
                    .thenAccept(bk -> {
                        final ManagedLedgerImpl newledger =
                                createManagedLedger(bk, store, name, config, mlOwnershipChecker);
                        PendingInitializeManagedLedger pendingLedger =
                                new PendingInitializeManagedLedger(newledger, future);
                        pendingInitializeLedgers.put(name, pendingLedger);
                        newledger.initialize(new ManagedLedgerInitializeLedgerCallback() {
                            @Override
                            public void initializeComplete() {
                                pendingLedger.ledgerInitialized = true;
                                // The future is already done also if the factory shutdown timed out waiting for
                                // an open whose initialization hadn't started yet, so it couldn't be claimed
                                if (pendingInitializeLedgers.get(name) != pendingLedger || future.isDone()) {
                                    // The initialization timed out or the factory was shut down while it was in
                                    // progress, and the future was failed and removed from the map. Close the
                                    // instance so that it doesn't keep running next to the one of a retried open.
                                    log.warn().attr("managedLedger", name)
                                        .attr("cost", System.currentTimeMillis() - startTimeOfCreation)
                                        .log("Managed ledger initialization completed after it was abandoned,"
                                                + " closing it");
                                    pendingInitializeLedgers.remove(name, pendingLedger);
                                    closePendingInitializeLedger(name, pendingLedger);
                                    future.completeExceptionally(new ManagedLedgerException(
                                            "Managed ledger initialization was abandoned"));
                                    return;
                                }
                                log.info().attr("managedLedger", name)
                                    .attr("cost", System.currentTimeMillis() - startTimeOfCreation)
                                    .log("Successfully initialize managed ledger");
                                // May need to update the cursor position and wait them finished
                                newledger.maybeUpdateCursorBeforeTrimmingConsumedLedger().whenComplete((__, ex) -> {
                                    // ignore ex since it is handled in maybeUpdateCursorBeforeTrimmingConsumedLedger
                                    // Keep the initialization claim until the cursor updates finish, so that an open
                                    // still waiting for them can be abandoned by a timeout or factory shutdown.
                                    if (!pendingInitializeLedgers.remove(name, pendingLedger)
                                            || !future.complete(newledger)) {
                                        closePendingInitializeLedger(name, pendingLedger);
                                        future.completeExceptionally(new ManagedLedgerException(
                                                "Managed ledger initialization was abandoned"));
                                        return;
                                    }
                                    // May need to trigger offloading
                                    if (config.isTriggerOffloadOnTopicLoad()) {
                                        newledger.maybeOffloadInBackground(AUTOMATIC_OFFLOAD_TRIGGER);
                                    }
                                });
                            }

                            @Override
                            public void initializeFailed(ManagedLedgerException e) {
                                if (config.isCreateIfMissing()) {
                                    log.error().attr("managedLedger", name)
                                        .exceptionMessage(e)
                                        .log("Failed to initialize managed ledger");
                                }

                                // Clean the map if initialization fails. The future is no longer in the map if the
                                // initialization timed out, and then the entry cache could already belong to the
                                // managed ledger of a retried open.
                                removeLedgerFuture(name, future);
                                // Close the instance also when the initialization timed out or the factory was shut
                                // down, since the instance is no longer closed elsewhere
                                pendingInitializeLedgers.remove(name, pendingLedger);
                                closePendingInitializeLedger(name, pendingLedger);

                                future.completeExceptionally(e);
                            }
                        }, null);
                    }).exceptionally(ex -> {
                        future.completeExceptionally(ex);
                        return null;
                    });
            return future;
        }).thenAccept(ml -> callback.openLedgerComplete(ml, ctx)).exceptionally(exception -> {
            callback.openLedgerFailed(ManagedLedgerException
                    .getManagedLedgerException(FutureUtil.unwrapCompletionException(exception)), ctx);
            return null;
        });
    }

    protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                                                    ManagedLedgerConfig config,
                                                    Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
        return config.getShadowSource() == null
                ? new ManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name, mlOwnershipChecker) :
                new ShadowManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name, mlOwnershipChecker);
    }

    @Override
    public void asyncOpenReadOnlyManagedLedger(String managedLedgerName,
                              AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback,
                              ManagedLedgerConfig config, Object ctx) {
        if (closed) {
            callback.openReadOnlyManagedLedgerFailed(
                    new ManagedLedgerException.ManagedLedgerFactoryClosedException(), ctx);
        }

        bookkeeperFactory
                .get(new EnsemblePlacementPolicyConfig(config.getBookKeeperEnsemblePlacementPolicyClassName(),
                        config.getBookKeeperEnsemblePlacementPolicyProperties()))
                .thenCompose(bk -> {
                    ReadOnlyManagedLedgerImplWrapper roManagedLedger = new ReadOnlyManagedLedgerImplWrapper(this, bk,
                            store, config, scheduledExecutor, managedLedgerName);
                    return roManagedLedger.initialize().thenApply(v -> roManagedLedger);
                }).thenAccept(roManagedLedger -> {
                    log.info().attr("managedLedger", managedLedgerName)
                            .log("Successfully initialize Read-only"
                                    + " managed ledger");
                    callback.openReadOnlyManagedLedgerComplete(roManagedLedger, ctx);
                }).exceptionally(e -> {
                    log.error().attr("managedLedger", managedLedgerName)
                            .exception(e)
                            .log("Failed to initialize Read-only"
                                    + " managed ledger");
                    callback.openReadOnlyManagedLedgerFailed(ManagedLedgerException
                            .getManagedLedgerException(FutureUtil.unwrapCompletionException(e)), ctx);
                    return null;
                });
    }

    @Override
    public ReadOnlyCursor openReadOnlyCursor(String managedLedgerName, Position startPosition,
                                             ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ReadOnlyCursor c = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncOpenReadOnlyCursor(managedLedgerName, startPosition, config, new OpenReadOnlyCursorCallback() {
            @Override
            public void openReadOnlyCursorComplete(ReadOnlyCursor cursor, Object ctx) {
                r.c = cursor;
                latch.countDown();
            }

            @Override
            public void openReadOnlyCursorFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.c;
    }

    @Override
    public void asyncOpenReadOnlyCursor(String managedLedgerName, Position startPosition, ManagedLedgerConfig config,
            OpenReadOnlyCursorCallback callback, Object ctx) {
        if (closed) {
            callback.openReadOnlyCursorFailed(new ManagedLedgerException.ManagedLedgerFactoryClosedException(), ctx);
            return;
        }
        AsyncCallbacks.OpenReadOnlyManagedLedgerCallback openReadOnlyManagedLedgerCallback =
                new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger readOnlyManagedLedger, Object ctx) {
                callback.openReadOnlyCursorComplete(readOnlyManagedLedger.
                        createReadOnlyCursor(startPosition), ctx);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                callback.openReadOnlyCursorFailed(exception, ctx);
            }
        };
        asyncOpenReadOnlyManagedLedger(managedLedgerName, openReadOnlyManagedLedgerCallback, config, null);
    }

    void close(ManagedLedger ledger) {
        // If the future in map is not done or has exceptionally complete, it means that @param-ledger is not in the
        // map.
        CompletableFuture<ManagedLedgerImpl> ledgerFuture = ledgers.get(ledger.getName());
        if (ledgerFuture == null || !ledgerFuture.isDone() || ledgerFuture.isCompletedExceptionally()){
            return;
        }
        if (ledgerFuture.join() != ledger){
            return;
        }
        // Remove the ledger from the internal factory cache.
        removeLedgerFuture(ledger.getName(), ledgerFuture);
    }

    /**
     * Removes the future of a managed ledger from the map, and its entry cache with it, if the future is still mapped.
     *
     * <p>The entry cache is keyed by the managed ledger name, so it's removed atomically with the future. Otherwise a
     * concurrent open could create a new managed ledger instance between the two removals, and the new instance would
     * either get the entry cache of the old instance or have its own entry cache removed.
     *
     * @return true if the future was removed
     */
    private boolean removeLedgerFuture(String name, CompletableFuture<ManagedLedgerImpl> future) {
        MutableBoolean removed = new MutableBoolean();
        ledgers.computeIfPresent(name, (__, currentFuture) -> {
            if (currentFuture != future) {
                return currentFuture;
            }
            entryCacheManager.removeEntryCache(name);
            removed.setTrue();
            return null;
        });
        return removed.booleanValue();
    }

    private CompletableFuture<Void> closePendingInitializeLedger(String name,
                                                                 PendingInitializeManagedLedger pendingLedger) {
        if (pendingLedger.closeStarted.compareAndSet(false, true)) {
            pendingLedger.closeFuture.whenComplete((__, exception) -> {
                if (exception != null) {
                    log.warn().attr("managedLedger", name).exception(exception)
                            .log("Failed to close a pending initialization managed ledger");
                }
            });
            try {
                pendingLedger.ledger.asyncClose(pendingLedger.closeFuture, null);
            } catch (Exception e) {
                pendingLedger.closeFuture.completeExceptionally(e);
            }
        }
        return pendingLedger.closeFuture;
    }

    public CompletableFuture<Void> shutdownAsync() throws ManagedLedgerException {
        if (closed) {
            throw new ManagedLedgerException.ManagedLedgerFactoryClosedException();
        }
        closed = true;

        statsTask.cancel(true);
        flushCursorsTask.cancel(true);
        readEntryTimeoutTracker.close();
        cacheEvictionExecutor.shutdownNow();

        CompletableFuture<Void> closeLedgersFuture = closeLedgersOnShutdown();
        CompletableFuture<BookKeeper> bookkeeperFuture = isBookkeeperManaged
                ? bookkeeperFactory.get()
                : CompletableFuture.completedFuture(null);
        return bookkeeperFuture
                .thenCompose(__ -> closeLedgersFuture)
                .whenCompleteAsync((__, ___) -> {
                    //wait for tasks in scheduledExecutor executed.
                    cancelSessionListener.run();
                    store.close();
                    openTelemetryManagedCursorStats.close();
                    openTelemetryManagedLedgerStats.close();
                    openTelemetryCacheStats.close();
                    scheduledExecutor.shutdownNow();
                    entryCacheManager.clear();
                    if (bookkeeperFactory instanceof DefaultBkFactory defaultBkFactory) {
                        try {
                            defaultBkFactory.close();
                        } catch (Exception e) {
                            log.warn().exception(e).log("Failed to close bookkeeper client");
                        }
                    }
                });
    }

    /**
     * Closes all the managed ledgers of the factory, then the ones that opens racing with the shutdown added in the
     * meantime, until none is left.
     */
    private CompletableFuture<Void> closeLedgersOnShutdown() {
        List<String> ledgerNames = new ArrayList<>(ledgers.keySet());
        if (ledgerNames.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        log.info().attr("numLedgers", ledgerNames.size()).log("Closing ledgers");
        List<CompletableFuture<Void>> futures = new ArrayList<>(ledgerNames.size());
        for (String ledgerName : ledgerNames) {
            CompletableFuture<ManagedLedgerImpl> ledgerFuture = ledgers.remove(ledgerName);
            if (ledgerFuture != null) {
                futures.add(closeLedgerOnShutdown(ledgerName, ledgerFuture));
            }
        }
        return Futures.waitForAll(futures).thenCompose(__ -> closeLedgersOnShutdown());
    }

    private CompletableFuture<Void> closeLedgerOnShutdown(String ledgerName,
                                                          CompletableFuture<ManagedLedgerImpl> ledgerFuture) {
        PendingInitializeManagedLedger pendingLedger = pendingInitializeLedgers.get(ledgerName);
        CompletableFuture<Void> future = new CompletableFuture<>();
        ledgerFuture.whenCompleteAsync((managedLedger, throwable) -> {
            if (throwable != null || managedLedger == null) {
                if (pendingLedger == null || pendingLedger.future != ledgerFuture || !pendingLedger.ledgerInitialized) {
                    future.complete(null);
                    return;
                }
            }
            // Cursor position updates can still be pending after the ledger itself initialized. Close that
            // instance while the scheduler is alive, even though shutdown has already failed its open future.
            ManagedLedgerImpl ledgerToClose = managedLedger != null ? managedLedger : pendingLedger.ledger;
            if (pendingLedger != null && pendingLedger.future == ledgerFuture) {
                // An abandonment callback may already be closing this instance. Wait for that close rather than
                // calling asyncClose again, which reports success as soon as the instance's state is Closed.
                closePendingInitializeLedger(ledgerName, pendingLedger)
                        .whenComplete((__, exception) -> future.complete(null));
                return;
            }
            ledgerToClose.asyncClose(new CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    future.complete(null);
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    log.warn().attr("managedLedger", ledgerToClose.getName()).exception(exception)
                            .log("Got exception when closing managed ledger");
                    future.complete(null);
                }
            }, null);

        }, scheduledExecutor.chooseThread());
        // Fail an open whose initialization is in progress directly to make sure all callbacks are called. Removing
        // the pending entry claims the initialization, so that the managed ledger instance is closed once its
        // initialization completes.
        if (pendingLedger != null && pendingLedger.future == ledgerFuture
                && pendingInitializeLedgers.remove(ledgerName, pendingLedger)) {
            pendingLedger.ledger.abandonInitialization();
            ledgerFuture.completeExceptionally(new ManagedLedgerException.ManagedLedgerFactoryClosedException());
        }
        // Bound the wait, so that a ledger that never finishes opening or closing cannot hang the shutdown
        return FutureUtil.addTimeoutHandling(future, Duration.ofMillis(shutdownLedgerCloseTimeoutMillis),
                        scheduledExecutor, () -> FutureUtil.createTimeoutException("Timed out closing managed ledger",
                                ManagedLedgerFactoryImpl.class, "closeLedgerOnShutdown"))
                .exceptionally(ex -> {
                    log.warn().attr("managedLedger", ledgerName).attr("timeoutMs", shutdownLedgerCloseTimeoutMillis)
                            .log("Timed out closing managed ledger on shutdown");
                    // Make sure that the callbacks of an open that is still in progress are called
                    ledgerFuture.completeExceptionally(
                            new ManagedLedgerException.ManagedLedgerFactoryClosedException());
                    return null;
                });
    }

    @VisibleForTesting
    void setShutdownLedgerCloseTimeoutMillis(long shutdownLedgerCloseTimeoutMillis) {
        this.shutdownLedgerCloseTimeoutMillis = shutdownLedgerCloseTimeoutMillis;
    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {
        try {
            shutdownAsync().get();
        } catch (ExecutionException e) {
            throw getManagedLedgerException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Boolean> asyncExists(String ledgerName) {
        return store.asyncExists(ledgerName);
    }

    @Override
    public org.apache.bookkeeper.mledger.ManagedLedgerInfo getManagedLedgerInfo(String name)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            org.apache.bookkeeper.mledger.ManagedLedgerInfo info = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncGetManagedLedgerInfo(name, new ManagedLedgerInfoCallback() {
            @Override
            public void getInfoComplete(org.apache.bookkeeper.mledger.ManagedLedgerInfo info, Object ctx) {
                r.info = info;
                latch.countDown();
            }

            @Override
            public void getInfoFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.info;
    }

    @Override
    public void asyncGetManagedLedgerInfo(String name, ManagedLedgerInfoCallback callback, Object ctx) {
        store.getManagedLedgerInfo(name, false /* createIfMissing */,
                new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo pbInfo, Stat stat) {
                org.apache.bookkeeper.mledger.ManagedLedgerInfo info =
                        new org.apache.bookkeeper.mledger.ManagedLedgerInfo();
                info.version = stat.getVersion();
                info.creationDate = DateFormatter.format(stat.getCreationTimestamp());
                info.modificationDate = DateFormatter.format(stat.getModificationTimestamp());

                info.ledgers = new ArrayList<>(pbInfo.getLedgerInfosCount());
                if (pbInfo.hasTerminatedPosition()) {
                    info.terminatedPosition = new org.apache.bookkeeper.mledger.ManagedLedgerInfo.PositionInfo();
                    info.terminatedPosition.ledgerId = pbInfo.getTerminatedPosition().getLedgerId();
                    info.terminatedPosition.entryId = pbInfo.getTerminatedPosition().getEntryId();
                }

                if (pbInfo.getPropertiesCount() > 0) {
                    info.properties = new TreeMap<>();
                    for (int i = 0; i < pbInfo.getPropertiesCount(); i++) {
                        KeyValue property = pbInfo.getPropertyAt(i);
                        info.properties.put(property.getKey(), property.getValue());
                    }
                }

                for (int i = 0; i < pbInfo.getLedgerInfosCount(); i++) {
                    LedgerInfo pbLedgerInfo = pbInfo.getLedgerInfoAt(i);
                    org.apache.bookkeeper.mledger.ManagedLedgerInfo.LedgerInfo ledgerInfo =
                            new org.apache.bookkeeper.mledger.ManagedLedgerInfo.LedgerInfo();
                    ledgerInfo.ledgerId = pbLedgerInfo.getLedgerId();
                    ledgerInfo.entries = pbLedgerInfo.hasEntries() ? pbLedgerInfo.getEntries() : null;
                    ledgerInfo.size = pbLedgerInfo.hasSize() ? pbLedgerInfo.getSize() : null;
                    ledgerInfo.timestamp = pbLedgerInfo.hasTimestamp() ? pbLedgerInfo.getTimestamp() : null;
                    ledgerInfo.isOffloaded = pbLedgerInfo.hasOffloadContext();
                    if (pbLedgerInfo.hasOffloadContext()) {
                        OffloadContext offloadContext = pbLedgerInfo.getOffloadContext();
                        UUID uuid = new UUID(offloadContext.getUidMsb(), offloadContext.getUidLsb());
                        ledgerInfo.offloadedContextUuid = uuid.toString();
                    }
                    info.ledgers.add(ledgerInfo);
                }

                store.getCursors(name, new MetaStoreCallback<List<String>>() {
                    @Override
                    public void operationComplete(List<String> cursorsList, Stat stat) {
                        // Get the info for each cursor
                        info.cursors = new ConcurrentSkipListMap<>();
                        List<CompletableFuture<Void>> cursorsFutures = new ArrayList<>();

                        for (String cursorName : cursorsList) {
                            CompletableFuture<Void> cursorFuture = new CompletableFuture<>();
                            cursorsFutures.add(cursorFuture);
                            store.asyncGetCursorInfo(name, cursorName,
                                    new MetaStoreCallback<ManagedCursorInfo>() {
                                        @Override
                                        public void operationComplete(ManagedCursorInfo pbCursorInfo, Stat stat) {
                                            CursorInfo cursorInfo = new CursorInfo();
                                            cursorInfo.version = stat.getVersion();
                                            cursorInfo.creationDate = DateFormatter.format(stat.getCreationTimestamp());
                                            cursorInfo.modificationDate = DateFormatter
                                                    .format(stat.getModificationTimestamp());

                                            cursorInfo.cursorsLedgerId = pbCursorInfo.getCursorsLedgerId();

                                            if (pbCursorInfo.hasMarkDeleteLedgerId()) {
                                                cursorInfo.markDelete =
                                                        new org.apache.bookkeeper.mledger.ManagedLedgerInfo
                                                                .PositionInfo();
                                                cursorInfo.markDelete.ledgerId = pbCursorInfo.getMarkDeleteLedgerId();
                                                cursorInfo.markDelete.entryId = pbCursorInfo.getMarkDeleteEntryId();
                                            }

                                            if (pbCursorInfo.getPropertiesCount() > 0) {
                                                cursorInfo.properties = new TreeMap<>();
                                                for (int i = 0; i < pbCursorInfo.getPropertiesCount(); i++) {
                                                    LongProperty property = pbCursorInfo.getPropertyAt(i);
                                                    cursorInfo.properties.put(property.getName(), property.getValue());
                                                }
                                            }

                                            if (pbCursorInfo.getIndividualDeletedMessagesCount() > 0) {
                                                cursorInfo.individualDeletedMessages = new ArrayList<>();
                                                for (int i = 0; i < pbCursorInfo
                                                        .getIndividualDeletedMessagesCount(); i++) {
                                                    MessageRange range =
                                                            pbCursorInfo.getIndividualDeletedMessageAt(i);
                                                    MessageRangeInfo rangeInfo = new MessageRangeInfo();
                                                    rangeInfo.from.ledgerId = range.getLowerEndpoint().getLedgerId();
                                                    rangeInfo.from.entryId = range.getLowerEndpoint().getEntryId();
                                                    rangeInfo.to.ledgerId = range.getUpperEndpoint().getLedgerId();
                                                    rangeInfo.to.entryId = range.getUpperEndpoint().getEntryId();
                                                    cursorInfo.individualDeletedMessages.add(rangeInfo);
                                                }
                                            }

                                            info.cursors.put(cursorName, cursorInfo);
                                            cursorFuture.complete(null);
                                        }

                                        @Override
                                        public void operationFailed(MetaStoreException e) {
                                            cursorFuture.completeExceptionally(e);
                                        }
                                    });
                        }

                        Futures.waitForAll(cursorsFutures).thenRun(() -> {
                            // Completed all the cursors info
                            callback.getInfoComplete(info, ctx);
                        }).exceptionally((ex) -> {
                            callback.getInfoFailed(getManagedLedgerException(ex.getCause()), ctx);
                            return null;
                        });
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        callback.getInfoFailed(e, ctx);
                    }
                });
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.getInfoFailed(e, ctx);
            }
        });
    }

    @Override
    public void delete(String name) throws InterruptedException, ManagedLedgerException {
        delete(name, CompletableFuture.completedFuture(null));
    }

    @Override
    public void delete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncDelete(name, mlConfigFuture, new DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                latch.countDown();
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
    }

    @Override
    public void asyncDelete(String name, DeleteLedgerCallback callback, Object ctx) {
        asyncDelete(name, CompletableFuture.completedFuture(null), callback, ctx);
    }

    @Override
    public void asyncDelete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                            DeleteLedgerCallback callback, Object ctx) {
        CompletableFuture<ManagedLedgerImpl> future = ledgers.get(name);
        if (future == null) {
            // Managed ledger does not exist and we're not currently trying to open it
            deleteManagedLedger(name, mlConfigFuture, callback, ctx);
        } else {
            future.thenAccept(ml -> {
                // If it's open, delete in the normal way
                ml.asyncDelete(callback, ctx);
            }).exceptionally(ex -> {
                // If it fails to get open, it will be cleaned by managed ledger opening error handling.
                // then retry will go to `future=null` branch.
                final Throwable rc = FutureUtil.unwrapCompletionException(ex);
                callback.deleteLedgerFailed(getManagedLedgerException(rc), ctx);
                return null;
            });
        }
    }

    /**
     * Delete all managed ledger resources and metadata.
     */
    void deleteManagedLedger(String managedLedgerName, CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                             DeleteLedgerCallback callback, Object ctx) {
        // Read the managed ledger metadata from store
        asyncGetManagedLedgerInfo(managedLedgerName, new ManagedLedgerInfoCallback() {
            @Override
            public void getInfoComplete(org.apache.bookkeeper.mledger.ManagedLedgerInfo info, Object ctx) {
                getBookKeeper().thenCompose(bk -> {
                    // First delete all cursors resources
                    List<CompletableFuture<Void>> futures = info.cursors.entrySet().stream()
                            .map(e -> deleteCursor(bk, managedLedgerName, e.getKey(), e.getValue()))
                            .collect(Collectors.toList());
                    return Futures.waitForAll(futures).thenApply(v -> bk);
                }).thenAccept(bk -> {
                    deleteManagedLedgerData(bk, managedLedgerName, info, mlConfigFuture, callback, ctx);
                }).exceptionally(ex -> {
                    callback.deleteLedgerFailed(new ManagedLedgerException(ex), ctx);
                    return null;
                });
            }

            @Override
            public void getInfoFailed(ManagedLedgerException exception, Object ctx) {
                callback.deleteLedgerFailed(exception, ctx);
            }
        }, ctx);
    }

    private void deleteManagedLedgerData(BookKeeper bkc, String managedLedgerName,
                                         org.apache.bookkeeper.mledger.ManagedLedgerInfo info,
                                         CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                                         DeleteLedgerCallback callback, Object ctx) {
        mlConfigFuture.handle((mlConfig, ex) -> mlConfig).thenAccept(mlConfig -> {
            if (isShadowManagedLedger(info, mlConfig)) {
                // The ledgers listed by a shadow managed ledger belong to its source managed ledger,
                // so only the metadata of the shadow managed ledger is removed.
                log.info().attr("managedLedger", managedLedgerName)
                        .log("Keeping ledgers of the source managed ledger while deleting shadow managed ledger");
                removeManagedLedgerMetadata(managedLedgerName, callback, ctx);
            } else {
                deleteOwnedManagedLedgerData(bkc, managedLedgerName, info, mlConfigFuture, callback, ctx);
            }
        }).exceptionally(ex -> {
            callback.deleteLedgerFailed(new ManagedLedgerException(ex), ctx);
            return null;
        });
    }

    /**
     * A managed ledger is a shadow managed ledger when its stored properties or the config supplied for its
     * deletion (for example resolved from the partitioned topic metadata) contain the shadow source property.
     */
    private static boolean isShadowManagedLedger(org.apache.bookkeeper.mledger.ManagedLedgerInfo info,
                                                 ManagedLedgerConfig mlConfig) {
        if (info.properties != null && info.properties.containsKey(ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY)) {
            return true;
        }
        return mlConfig != null && mlConfig.getShadowSource() != null;
    }

    private void deleteOwnedManagedLedgerData(BookKeeper bkc, String managedLedgerName,
                                              org.apache.bookkeeper.mledger.ManagedLedgerInfo info,
                                              CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                                              DeleteLedgerCallback callback, Object ctx) {
        final CompletableFuture<Map<Long, LedgerInfo>>
                ledgerInfosFuture = new CompletableFuture<>();
        store.getManagedLedgerInfo(managedLedgerName, false, null,
                new MetaStoreCallback<>() {
                    @Override
                    public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                        Map<Long, LedgerInfo> infos = new HashMap<>();
                        for (int i = 0; i < mlInfo.getLedgerInfosCount(); i++) {
                            LedgerInfo ls = mlInfo.getLedgerInfoAt(i);
                            infos.put(ls.getLedgerId(), ls);
                        }
                        ledgerInfosFuture.complete(infos);
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        log.error().attr("managedLedger", managedLedgerName)
                                .exception(e)
                                .log("Failed to get managed ledger info");
                        ledgerInfosFuture.completeExceptionally(e);
                    }
                });

        Futures.waitForAll(info.ledgers.stream()
                .map(li -> {
                    final CompletableFuture<Void> res;
                    if (li.isOffloaded) {
                        res = mlConfigFuture
                                .thenCombine(ledgerInfosFuture, Pair::of)
                                .thenCompose(pair -> {
                            ManagedLedgerConfig mlConfig =  pair.getLeft();
                            Map<Long, LedgerInfo> ledgerInfos = pair.getRight();

                            if (mlConfig == null || ledgerInfos == null) {
                                return CompletableFuture.completedFuture(null);
                            }

                            LedgerInfo ls = ledgerInfos.get(li.ledgerId);

                            if (ls.getOffloadContext().hasUidMsb()) {
                                LedgerInfo newInfo = new LedgerInfo();
                                newInfo.copyFrom(ls);
                                newInfo.setOffloadContext().setBookkeeperDeleted(true);
                                String driverName = OffloadUtils.getOffloadDriverName(ls,
                                        mlConfig.getLedgerOffloader().getOffloadDriverName());
                                Map<String, String> driverMetadata = OffloadUtils.getOffloadDriverMetadata(ls,
                                        mlConfig.getLedgerOffloader().getOffloadDriverMetadata());
                                OffloadUtils.setOffloadDriverMetadata(newInfo, driverName, driverMetadata);

                                UUID uuid = new UUID(ls.getOffloadContext().getUidMsb(),
                                        ls.getOffloadContext().getUidLsb());
                                return OffloadUtils.cleanupOffloaded(li.ledgerId, uuid, mlConfig,
                                        OffloadUtils.getOffloadDriverMetadata(ls,
                                                mlConfig.getLedgerOffloader().getOffloadDriverMetadata()),
                                        "Deletion", managedLedgerName, scheduledExecutor);
                            }

                            return CompletableFuture.completedFuture(null);
                        });
                    } else {
                        res = CompletableFuture.completedFuture(null);
                    }
                    return res.thenCompose(__ -> bkc.newDeleteLedgerOp().withLedgerId(li.ledgerId).execute()
                            .handle((result, ex) -> {
                                if (ex != null) {
                                    int rc = BKException.getExceptionCode(ex);
                                    if (Errors.isNoSuchLedgerExistsException(rc)) {
                                        log.info().attr("ledgerId", li.ledgerId).log("Ledger does not exist, ignoring");
                                        return null;
                                    }
                                    throw new CompletionException(ex);
                                }
                                return result;
                        }));
                })
                .collect(Collectors.toList()))
                .thenRun(() -> {
                    // Delete the metadata
                    removeManagedLedgerMetadata(managedLedgerName, callback, ctx);
                }).exceptionally(ex -> {
                    callback.deleteLedgerFailed(new ManagedLedgerException(ex), ctx);
                    return null;
                });
    }

    private void removeManagedLedgerMetadata(String managedLedgerName, DeleteLedgerCallback callback, Object ctx) {
        store.removeManagedLedger(managedLedgerName, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                callback.deleteLedgerComplete(ctx);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.deleteLedgerFailed(new ManagedLedgerException(e), ctx);
            }
        });
    }

    private CompletableFuture<Void> deleteCursor(BookKeeper bkc, String managedLedgerName, String cursorName,
                                                 CursorInfo cursor) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Void> cursorLedgerDeleteFuture;

        // Delete the cursor ledger if present
        if (cursor.cursorsLedgerId != -1) {
            cursorLedgerDeleteFuture = bkc.newDeleteLedgerOp().withLedgerId(cursor.cursorsLedgerId)
                    .execute()
                    .handle((result, ex) -> {
                        if (ex != null) {
                            int rc = BKException.getExceptionCode(ex);
                            if (Errors.isNoSuchLedgerExistsException(rc)) {
                                log.info().attr("ledgerId", cursor.cursorsLedgerId)
                                        .log("Ledger does not exist, ignoring");
                                return null;
                            }
                            throw new CompletionException(ex);
                        }
                        return result;
                    });
        } else {
            cursorLedgerDeleteFuture = CompletableFuture.completedFuture(null);
        }

        cursorLedgerDeleteFuture.thenRun(() -> {
            store.asyncRemoveCursor(managedLedgerName, cursorName, new MetaStoreCallback<Void>() {
                @Override
                public void operationComplete(Void result, Stat stat) {
                    future.complete(null);
                }

                @Override
                public void operationFailed(MetaStoreException e) {
                   future.completeExceptionally(e);
                }
            });
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });

        return future;
    }

    @Override
    public CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name) {
        return store.getManagedLedgerPropertiesAsync(name);
    }

    public MetaStore getMetaStore() {
        return store;
    }

    @Override
    public ManagedLedgerFactoryConfig getConfig() {
        return config;
    }

    @Override
    public EntryCacheManager getEntryCacheManager() {
        return entryCacheManager;
    }

    @Override
    public void updateCacheEvictionTimeThreshold(long cacheEvictionTimeThresholdNanos){
        this.cacheEvictionTimeThresholdNanos = cacheEvictionTimeThresholdNanos;
    }

    @Override
    public long getCacheEvictionTimeThreshold(){
        return cacheEvictionTimeThresholdNanos;
    }

    @Override
    public void updateCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(
            int extendTTLOfEntriesWithRemainingExpectedReadsMaxTimes) {
        entryCacheManager.updateCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(
                extendTTLOfEntriesWithRemainingExpectedReadsMaxTimes);
    }

    @Override
    public void updateCacheEvictionExtendTTLOfRecentlyAccessed(boolean cacheEvictionExtendTTLOfRecentlyAccessed) {
        entryCacheManager.updateCacheEvictionExtendTTLOfRecentlyAccessed(cacheEvictionExtendTTLOfRecentlyAccessed);
    }

    @Override
    public ManagedLedgerFactoryMXBean getCacheStats() {
        return this.mbean;
    }

    public CompletableFuture<BookKeeper> getBookKeeper() {
        return bookkeeperFactory.get();
    }

    private record CursorBacklog(String cursorName, long ledgerId, long backlog, boolean requiresRecovery) {
    }

    private record OfflineTopicBacklog(NavigableMap<Long, LedgerInfo> ledgers, List<CursorBacklog> cursors) {
    }

    @Override
    public void estimateUnloadedTopicBacklog(PersistentOfflineTopicStats offlineTopicStats,
                                             TopicName topicName, boolean accurate, Object ctx) throws Exception {
        List<?> credentials = (List<?>) ctx;
        BookKeeper.DigestType digestType = (BookKeeper.DigestType) credentials.get(0);
        byte[] password = (byte[]) credentials.get(1);
        CompletableFuture<OfflineTopicBacklog> computation = new CompletableFuture<>();
        if (!accurate) {
            computation.orTimeout(META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        FutureUtil.completeAfter(computation,
                readOfflineTopicBacklogAsync(topicName, digestType, password, computation));
        OfflineTopicBacklog result = computation.get();
        // A synchronous caller can perform the rare unconfirmed cursor reads on its own thread.
        List<CursorBacklog> cursors = accurate
                ? recoverCursorBacklogs(result.cursors(), result.ledgers(), digestType, password) : result.cursors();
        publishOfflineTopicBacklog(offlineTopicStats, result.ledgers(), cursors, accurate);
    }

    @Override
    public CompletableFuture<Void> estimateUnloadedTopicBacklogAsync(PersistentOfflineTopicStats offlineTopicStats,
            TopicName topicName, boolean accurate, Object ctx, Executor blockingExecutor) {
        return estimateUnloadedTopicBacklogAsync(offlineTopicStats, topicName, accurate, ctx, blockingExecutor,
                META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    CompletableFuture<Void> estimateUnloadedTopicBacklogAsync(PersistentOfflineTopicStats offlineTopicStats,
            TopicName topicName, boolean accurate, Object ctx, Executor blockingExecutor,
            long timeout, TimeUnit timeoutUnit) {
        try {
            Objects.requireNonNull(offlineTopicStats, "offlineTopicStats");
            Objects.requireNonNull(topicName, "topicName");
            Objects.requireNonNull(blockingExecutor, "blockingExecutor");
            List<?> credentials = (List<?>) ctx;
            BookKeeper.DigestType digestType = (BookKeeper.DigestType) credentials.get(0);
            byte[] password = (byte[]) credentials.get(1);
            CompletableFuture<OfflineTopicBacklog> computation = new CompletableFuture<>();
            if (!accurate) {
                computation.orTimeout(timeout, timeoutUnit);
            }
            FutureUtil.completeAfter(computation,
                    readOfflineTopicBacklogAsync(topicName, digestType, password, computation).thenCompose(result -> {
                        checkOfflineBacklogRequest(computation);
                        if (!accurate || result.cursors().stream().noneMatch(CursorBacklog::requiresRecovery)) {
                            return CompletableFuture.completedFuture(result);
                        }
                        // Unconfirmed reads wait synchronously and must stay off metadata and BK callback threads.
                        return getBookKeeper().thenCompose(bk -> CompletableFuture.supplyAsync(() -> {
                            checkOfflineBacklogRequest(computation);
                            return new OfflineTopicBacklog(result.ledgers(), recoverCursorBacklogs(
                                    bk, result.cursors(), result.ledgers(), digestType, password));
                        }, blockingExecutor));
                    }));
            // Only publish after timeout has been resolved. Late callbacks still release their resources.
            CompletableFuture<Void> published = computation.thenAccept(result -> publishOfflineTopicBacklog(
                    offlineTopicStats, result.ledgers(), result.cursors(), accurate));
            published.whenComplete((ignored, error) -> {
                if (published.isCancelled()) {
                    computation.cancel(false);
                }
            });
            return published;
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static void checkOfflineBacklogRequest(CompletableFuture<?> request) {
        if (request.isDone()) {
            throw new CancellationException("Offline backlog estimation has completed");
        }
    }

    private CompletableFuture<OfflineTopicBacklog> readOfflineTopicBacklogAsync(TopicName topicName,
            BookKeeper.DigestType digestType, byte[] password, CompletableFuture<?> request) {
        return getManagedLedgersInfoAsync(topicName, digestType, password, request)
                .thenCompose(ledgers -> readCursorBacklogsAsync(topicName, ledgers, digestType, password, request)
                        .thenApply(cursors -> new OfflineTopicBacklog(ledgers, cursors)));
    }

    private CompletableFuture<NavigableMap<Long, LedgerInfo>> getManagedLedgersInfoAsync(
            TopicName topicName, BookKeeper.DigestType digestType, byte[] password, CompletableFuture<?> request) {
        CompletableFuture<ManagedLedgerInfo> metadata = new CompletableFuture<>();
        try {
            checkOfflineBacklogRequest(request);
            getMetaStore().getManagedLedgerInfo(topicName.getPersistenceNamingEncoding(), false,
                    new MetaStoreCallback<ManagedLedgerInfo>() {
                        @Override
                        public void operationComplete(ManagedLedgerInfo info, Stat stat) {
                            metadata.complete(info);
                        }

                        @Override
                        public void operationFailed(MetaStoreException e) {
                            if (e instanceof MetadataNotFoundException) {
                                // Preserve the previous behavior for missing managed-ledger metadata: publish an empty
                                // snapshot so an offline backlog request returns zero instead of failing.
                                metadata.complete(new ManagedLedgerInfo());
                            } else {
                                metadata.completeExceptionally(e);
                            }
                        }
                    });
        } catch (Exception e) {
            metadata.completeExceptionally(e);
        }
        return metadata.thenCompose(info -> {
            checkOfflineBacklogRequest(request);
            NavigableMap<Long, LedgerInfo> ledgers = new TreeMap<>();
            for (int i = 0; i < info.getLedgerInfosCount(); i++) {
                LedgerInfo ledger = info.getLedgerInfoAt(i);
                ledgers.put(ledger.getLedgerId(), ledger);
            }
            if (ledgers.isEmpty()) {
                return CompletableFuture.completedFuture(ledgers);
            }
            long ledgerId = ledgers.lastKey();
            return getBookKeeper().thenCompose(bk -> {
                checkOfflineBacklogRequest(request);
                return bk.newOpenLedgerOp()
                        .withRecovery(false)
                        .withLedgerId(ledgerId)
                        .withDigestType(digestType.toApiDigestType())
                        .withPassword(password)
                        .withLoggerContext(log.with().attr("managedLedger",
                                topicName.getPersistenceNamingEncoding()).build())
                        .execute();
            }).handle((handle, error) -> {
                if (error != null) {
                    if (Errors.isNoSuchLedgerExistsException(BKException.getExceptionCode(error))) {
                        ledgers.remove(ledgerId);
                        return CompletableFuture.completedFuture(ledgers);
                    }
                    return CompletableFuture.<NavigableMap<Long, LedgerInfo>>failedFuture(error);
                }
                CompletableFuture<NavigableMap<Long, LedgerInfo>> result;
                try {
                    ledgers.put(ledgerId, new LedgerInfo().setLedgerId(ledgerId)
                            .setEntries(handle.getLastAddConfirmed() + 1).setSize(handle.getLength())
                            .setTimestamp(System.currentTimeMillis()));
                    result = CompletableFuture.completedFuture(ledgers);
                } catch (Exception e) {
                    result = CompletableFuture.failedFuture(e);
                }
                return closeLedgerAfter(result, handle);
            }).thenCompose(result -> result);
        });
    }

    public void calculateCursorBacklogs(TopicName topicName, NavigableMap<Long, LedgerInfo> ledgers,
            PersistentOfflineTopicStats offlineTopicStats, boolean accurate,
            BookKeeper.DigestType digestType, byte[] password) throws Exception {
        if (ledgers.isEmpty()) {
            return;
        }
        NavigableMap<Long, LedgerInfo> ledgerSnapshot = new TreeMap<>(ledgers);
        CompletableFuture<List<CursorBacklog>> computation = new CompletableFuture<>();
        if (!accurate) {
            computation.orTimeout(META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        FutureUtil.completeAfter(computation,
                readCursorBacklogsAsync(topicName, ledgerSnapshot, digestType, password, computation));
        List<CursorBacklog> cursors = computation.get();
        if (accurate) {
            cursors = recoverCursorBacklogs(cursors, ledgerSnapshot, digestType, password);
        }
        publishCursorBacklogs(offlineTopicStats, cursors);
    }

    private CompletableFuture<List<CursorBacklog>> readCursorBacklogsAsync(TopicName topicName,
            NavigableMap<Long, LedgerInfo> ledgers, BookKeeper.DigestType digestType, byte[] password,
            CompletableFuture<?> request) {
        if (ledgers.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        CompletableFuture<List<String>> cursorNames = new CompletableFuture<>();
        try {
            checkOfflineBacklogRequest(request);
            getMetaStore().getCursors(topicName.getPersistenceNamingEncoding(),
                    new MetaStoreCallback<List<String>>() {
                        @Override
                        public void operationComplete(List<String> cursors, Stat stat) {
                            cursorNames.complete(cursors);
                        }

                        @Override
                        public void operationFailed(MetaStoreException e) {
                            cursorNames.completeExceptionally(e);
                        }
                    });
        } catch (Exception e) {
            cursorNames.completeExceptionally(e);
        }
        return cursorNames.thenCompose(names -> {
            checkOfflineBacklogRequest(request);
            if (names.isEmpty()) {
                return CompletableFuture.completedFuture(List.of());
            }
            return getBookKeeper().thenCompose(bk -> {
                checkOfflineBacklogRequest(request);
                List<CompletableFuture<CursorBacklog>> cursors = new ArrayList<>(names.size());
                for (String cursor : names) {
                    if (request.isDone()) {
                        break;
                    }
                    cursors.add(readCursorBacklogAsync(topicName.getPersistenceNamingEncoding(), cursor,
                            ledgers, bk, digestType, password, request));
                }
                return CompletableFuture.allOf(cursors.toArray(CompletableFuture[]::new))
                        .thenApply(__ -> cursors.stream().map(cursor -> cursor.getNow(null)).toList());
            });
        });
    }

    private CompletableFuture<CursorBacklog> readCursorBacklogAsync(String managedLedgerName, String cursorName,
            NavigableMap<Long, LedgerInfo> ledgers, BookKeeper bk,
            BookKeeper.DigestType digestType, byte[] password, CompletableFuture<?> request) {
        CompletableFuture<ManagedCursorInfo> metadata = new CompletableFuture<>();
        try {
            checkOfflineBacklogRequest(request);
            getMetaStore().asyncGetCursorInfo(managedLedgerName, cursorName,
                    new MetaStoreCallback<ManagedCursorInfo>() {
                        @Override
                        public void operationComplete(ManagedCursorInfo info, Stat stat) {
                            metadata.complete(info);
                        }

                        @Override
                        public void operationFailed(MetaStoreException e) {
                            metadata.completeExceptionally(e);
                        }
                    });
        } catch (Exception e) {
            metadata.completeExceptionally(e);
        }
        return metadata.thenCompose(info -> {
            checkOfflineBacklogRequest(request);
            long ledgerId = info.getCursorsLedgerId();
            if (ledgerId == -1) {
                Position position = PositionFactory.create(info.getMarkDeleteLedgerId(), info.getMarkDeleteEntryId());
                return CompletableFuture.completedFuture(cursorBacklog(cursorName, ledgerId, position, ledgers));
            }
            return bk.newOpenLedgerOp()
                    .withRecovery(false)
                    .withLedgerId(ledgerId)
                    .withDigestType(digestType.toApiDigestType())
                    .withPassword(password)
                    .withLoggerContext(log.with().attr("managedLedger", managedLedgerName).build())
                    .execute().thenCompose(handle -> {
                        CompletableFuture<CursorBacklog> result;
                        try {
                            checkOfflineBacklogRequest(request);
                            long entryId = handle.getLastAddConfirmed();
                            if (entryId == LedgerHandle.INVALID_ENTRY_ID) {
                                result = CompletableFuture.completedFuture(
                                        new CursorBacklog(cursorName, ledgerId, -1, true));
                            } else {
                                result = handle.readAsync(entryId, entryId).thenApply(entries -> {
                                    try (LedgerEntries ledgerEntries = entries) {
                                        checkOfflineBacklogRequest(request);
                                        PositionInfo positionInfo = new PositionInfo();
                                        positionInfo.parseFrom(ledgerEntries.getEntry(entryId).getEntryBytes());
                                        Position position = PositionFactory.create(
                                                positionInfo.getLedgerId(), positionInfo.getEntryId());
                                        return cursorBacklog(cursorName, ledgerId, position, ledgers);
                                    }
                                });
                            }
                        } catch (Exception e) {
                            result = CompletableFuture.failedFuture(e);
                        }
                        return closeLedgerAfter(result, handle);
                    });
        });
    }

    private <T> CompletableFuture<T> closeLedgerAfter(CompletableFuture<T> operation, ReadHandle handle) {
        return operation.handle((value, error) -> {
            CompletableFuture<Void> close;
            try {
                close = handle.closeAsync();
            } catch (Exception e) {
                close = CompletableFuture.failedFuture(e);
            }
            return close.handle((ignored, closeError) -> {
                if (error != null) {
                    throw new CompletionException(error);
                }
                if (closeError != null) {
                    throw new CompletionException(closeError);
                }
                return value;
            });
        }).thenCompose(result -> result);
    }

    private CursorBacklog cursorBacklog(String cursorName, long ledgerId, Position position,
            NavigableMap<Long, LedgerInfo> ledgers) {
        LedgerInfo lastLedger = ledgers.lastEntry().getValue();
        Position lastPosition = PositionFactory.create(lastLedger.getLedgerId(), lastLedger.getEntries() - 1);
        long backlog = position.compareTo(lastPosition) >= 0 ? 0
                : getNumberOfEntries(Range.openClosed(position, lastPosition), ledgers);
        return new CursorBacklog(cursorName, ledgerId, backlog, false);
    }

    private List<CursorBacklog> recoverCursorBacklogs(List<CursorBacklog> cursors,
            NavigableMap<Long, LedgerInfo> ledgers,
            BookKeeper.DigestType digestType, byte[] password) throws Exception {
        if (cursors.stream().noneMatch(CursorBacklog::requiresRecovery)) {
            return cursors;
        }
        return recoverCursorBacklogs(getBookKeeper().get(), cursors, ledgers, digestType, password);
    }

    private List<CursorBacklog> recoverCursorBacklogs(BookKeeper bk, List<CursorBacklog> cursors,
            NavigableMap<Long, LedgerInfo> ledgers, BookKeeper.DigestType digestType, byte[] password) {
        List<CursorBacklog> recovered = new ArrayList<>(cursors.size());
        for (CursorBacklog cursor : cursors) {
            if (!cursor.requiresRecovery()) {
                recovered.add(cursor);
                continue;
            }
            Position position = tryGetMDPosition(bk, cursor.ledgerId(), cursor.cursorName(), digestType, password);
            if (position == null) {
                throw new CompletionException(new ManagedLedgerException(
                        "Unable to determine position for cursor " + cursor.cursorName()));
            }
            recovered.add(cursorBacklog(cursor.cursorName(), cursor.ledgerId(), position, ledgers));
        }
        return recovered;
    }

    private void publishOfflineTopicBacklog(PersistentOfflineTopicStats stats,
            NavigableMap<Long, LedgerInfo> ledgers, List<CursorBacklog> cursors, boolean accurate) {
        stats.totalMessages = 0;
        stats.storageSize = 0;
        stats.dataLedgerDetails.clear();
        for (LedgerInfo ledger : ledgers.values()) {
            stats.totalMessages += ledger.getEntries();
            stats.storageSize += ledger.getSize();
            if (accurate) {
                stats.addLedgerDetails(ledger.getEntries(), ledger.getTimestamp(),
                        ledger.getSize(), ledger.getLedgerId());
            }
        }
        stats.messageBacklog = 0;
        stats.cursorDetails.clear();
        publishCursorBacklogs(stats, cursors);
        stats.statGeneratedAt.setTime(System.currentTimeMillis());
    }

    private void publishCursorBacklogs(PersistentOfflineTopicStats stats, List<CursorBacklog> cursors) {
        for (CursorBacklog cursor : cursors) {
            if (!cursor.requiresRecovery()) {
                stats.messageBacklog += cursor.backlog();
            }
            stats.addCursorDetails(cursor.cursorName(), cursor.backlog(), cursor.ledgerId());
        }
    }

    // need a better way than to duplicate the functionality below from ML
    private long getNumberOfEntries(Range<Position> range,
                                    NavigableMap<Long, LedgerInfo> ledgers) {
        Position fromPosition = range.lowerEndpoint();
        boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
        Position toPosition = range.upperEndpoint();
        boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;

        if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
            // If the 2 positions are in the same ledger
            long count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
            count += fromIncluded ? 1 : 0;
            count += toIncluded ? 1 : 0;
            return count;
        } else {
            long count = 0;
            // If the from & to are pointing to different ledgers, then we need to :
            // 1. Add the entries in the ledger pointed by toPosition
            count += toPosition.getEntryId();
            count += toIncluded ? 1 : 0;

            // 2. Add the entries in the ledger pointed by fromPosition
            LedgerInfo li = ledgers.get(fromPosition.getLedgerId());
            if (li != null) {
                count += li.getEntries() - (fromPosition.getEntryId() + 1);
                count += fromIncluded ? 1 : 0;
            }

            // 3. Add the whole ledgers entries in between
            for (LedgerInfo ls : ledgers
                    .subMap(fromPosition.getLedgerId(), false, toPosition.getLedgerId(), false).values()) {
                count += ls.getEntries();
            }

            return count;
        }
    }


    @VisibleForTesting
    Position tryGetMDPosition(BookKeeper bookKeeper, long ledgerId, String cursorName,
                              BookKeeper.DigestType digestType, byte[] password) {
        ReadHandle readHandle = null;
        Position lastAckedMessagePosition = null;
        try {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            readHandle = awaitOrClose(bookKeeper.newOpenLedgerOp()
                    .withRecovery(false)
                    .withLedgerId(ledgerId)
                    .withDigestType(digestType.toApiDigestType())
                    .withPassword(password)
                    .withLoggerContext(log.with().attr("cursor", cursorName).build())
                    .execute(), this::closeBacklogRecoveryHandle);
            // As with BookKeeperAdmin's iterator, recheck LAC after every read: an open cursor ledger
            // can reveal a newer LAC while reading entries.
            for (long entryId = 0; entryId <= readHandle.getLastAddConfirmed(); entryId++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                try (LedgerEntries entries = awaitOrClose(readHandle.readUnconfirmedAsync(entryId, entryId),
                        LedgerEntries::close)) {
                    PositionInfo positionInfo = new PositionInfo();
                    positionInfo.parseFrom(entries.getEntry(entryId).getEntryBytes());
                    lastAckedMessagePosition =
                            PositionFactory.create(positionInfo.getLedgerId(), positionInfo.getEntryId());
                } catch (ExecutionException e) {
                    if (BKException.getExceptionCode(e) == BKException.Code.NoSuchEntryException) {
                        break;
                    }
                    throw e;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            lastAckedMessagePosition = null;
        } catch (Exception e) {
            log.warn().attr("ledgerId", ledgerId)
                    .attr("cursor", cursorName)
                    .exception(e)
                    .log("Unable to determine LAC");
            lastAckedMessagePosition = null;
        } finally {
            if (readHandle != null) {
                closeBacklogRecoveryHandle(readHandle);
            }
        }
        return lastAckedMessagePosition;
    }

    private void closeBacklogRecoveryHandle(ReadHandle handle) {
        CompletableFuture<Void> closing;
        try {
            closing = handle.closeAsync();
        } catch (Exception e) {
            closing = CompletableFuture.failedFuture(e);
        }
        closing.exceptionally(error -> {
            log.warn().attr("ledgerId", handle.getId())
                    .exception(error).log("Unable to close cursor read handle");
            return null;
        });
    }

    private static <T> T awaitOrClose(CompletableFuture<T> future, Consumer<T> close)
            throws InterruptedException, ExecutionException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            // Interrupting the wait does not cancel BookKeeper IO. Release its eventual result,
            // which the interrupted caller can no longer receive.
            future.thenAccept(close);
            throw e;
        }
    }

    private static final int META_READ_TIMEOUT_SECONDS = 60;

    /**
     * Factory to create Bookkeeper-client for a given ensemblePlacementPolicy.
     *
     */
    public interface BookkeeperFactoryForCustomEnsemblePlacementPolicy {
        default CompletableFuture<BookKeeper> get() {
            return get(null);
        }

        /**
         * Returns Bk-Client for a given ensemblePlacementPolicyMetadata. It returns default bK-client if
         * ensemblePlacementPolicyMetadata is null.
         *
         * @param ensemblePlacementPolicyMetadata
         * @return
         */
        CompletableFuture<BookKeeper> get(EnsemblePlacementPolicyConfig ensemblePlacementPolicyMetadata);
    }

}

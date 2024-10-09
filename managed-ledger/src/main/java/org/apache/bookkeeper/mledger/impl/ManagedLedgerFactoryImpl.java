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
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.NULL_OFFLOAD_PROMISE;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.base.Predicates;
import com.google.common.collect.BoundType;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
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
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.CursorInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.MessageRangeInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.PositionInfo;
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
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.LongProperty;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.MessageRange;
import org.apache.bookkeeper.mledger.util.Errors;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerFactoryImpl implements ManagedLedgerFactory {
    private final MetaStore store;
    private final BookkeeperFactoryForCustomEnsemblePlacementPolicy bookkeeperFactory;
    private final boolean isBookkeeperManaged;
    private final ManagedLedgerFactoryConfig config;
    @Getter
    protected final OrderedScheduler scheduledExecutor;
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

    private volatile long cacheEvictionTimeThresholdNanos;
    private final MetadataStore metadataStore;

    private final OpenTelemetryManagedLedgerCacheStats openTelemetryCacheStats;
    private final OpenTelemetryManagedLedgerStats openTelemetryManagedLedgerStats;
    private final OpenTelemetryManagedCursorStats openTelemetryManagedCursorStats;

    //indicate whether shutdown() is called.
    private volatile boolean closed;

    /**
     * Keep a flag to indicate whether we're currently connected to the metadata service.
     */
    @Getter
    private boolean metadataServiceAvailable;

    private static class PendingInitializeManagedLedger {

        private final ManagedLedgerImpl ledger;
        private final long createTimeMs;

        PendingInitializeManagedLedger(ManagedLedgerImpl ledger) {
            this.ledger = ledger;
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
                true /* isBookkeeperManaged */, config, NullStatsLogger.INSTANCE, OpenTelemetry.noop());
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, BookKeeper bookKeeper)
            throws Exception {
        this(metadataStore, bookKeeper, new ManagedLedgerFactoryConfig());
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, BookKeeper bookKeeper,
                                    ManagedLedgerFactoryConfig config)
            throws Exception {
        this(metadataStore, (policyConfig) -> CompletableFuture.completedFuture(bookKeeper), config);
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                    BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                    ManagedLedgerFactoryConfig config)
            throws Exception {
        this(metadataStore, bookKeeperGroupFactory, false /* isBookkeeperManaged */,
                config, NullStatsLogger.INSTANCE, OpenTelemetry.noop());
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                    BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                    ManagedLedgerFactoryConfig config, StatsLogger statsLogger,
                                    OpenTelemetry openTelemetry)
            throws Exception {
        this(metadataStore, bookKeeperGroupFactory, false /* isBookkeeperManaged */,
                config, statsLogger, openTelemetry);
    }

    private ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                     BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                     boolean isBookkeeperManaged,
                                     ManagedLedgerFactoryConfig config,
                                     StatsLogger statsLogger,
                                     OpenTelemetry openTelemetry) throws Exception {
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
        this.entryCacheManager = new RangeEntryCacheManagerImpl(this, openTelemetry);
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

        metadataStore.registerSessionListener(this::handleMetadataStoreNotification);

        openTelemetryCacheStats = new OpenTelemetryManagedLedgerCacheStats(openTelemetry, this);
        openTelemetryManagedLedgerStats = new OpenTelemetryManagedLedgerStats(openTelemetry, this);
        openTelemetryManagedCursorStats = new OpenTelemetryManagedCursorStats(openTelemetry, this);
    }

    static class DefaultBkFactory implements BookkeeperFactoryForCustomEnsemblePlacementPolicy {

        private final BookKeeper bkClient;

        public DefaultBkFactory(ClientConfiguration bkClientConfiguration)
                throws InterruptedException, BKException, IOException {
            bkClient = new BookKeeper(bkClientConfiguration);
        }

        @Override
        public CompletableFuture<BookKeeper> get(EnsemblePlacementPolicyConfig policy) {
            return CompletableFuture.completedFuture(bkClient);
        }
    }

    private synchronized void handleMetadataStoreNotification(SessionEvent e) {
        log.info("Received MetadataStore session event: {}", e);
        metadataServiceAvailable = e.isConnected();
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

    private synchronized void doCacheEviction() {
        long maxTimestamp = System.nanoTime() - cacheEvictionTimeThresholdNanos;

        ledgers.values().forEach(mlfuture -> {
            if (mlfuture.isDone() && !mlfuture.isCompletedExceptionally()) {
                ManagedLedgerImpl ml = mlfuture.getNow(null);
                if (ml != null) {
                    ml.doCacheEviction(maxTimestamp);
                }
            }
        });
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
        return open(name, new ManagedLedgerConfig());
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
        asyncOpen(name, new ManagedLedgerConfig(), callback, null, ctx);
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
                        log.warn("[{}] Attempted to open ledger in {} state. Removing from the map to recreate it",
                                name, l.getState());
                        ledgers.remove(name, existingFuture);
                    }
                } catch (Exception e) {
                    // Unable to get the future
                    log.warn("[{}] Got exception while trying to retrieve ledger", name, e);
                }
            } else {
                PendingInitializeManagedLedger pendingLedger = pendingInitializeLedgers.get(name);
                if (null != pendingLedger) {
                    long pendingMs = System.currentTimeMillis() - pendingLedger.createTimeMs;
                    if (pendingMs > TimeUnit.SECONDS.toMillis(config.getMetadataOperationsTimeoutSeconds())) {
                        log.warn("[{}] Managed ledger has been pending in initialize state more than {} milliseconds,"
                            + " remove it from cache to retry ...", name, pendingMs);
                        ledgers.remove(name, existingFuture);
                        pendingInitializeLedgers.remove(name, pendingLedger);
                    }
                }

            }
        }

        // Ensure only one managed ledger is created and initialized
        ledgers.computeIfAbsent(name, (mlName) -> {
            // Create the managed ledger
            CompletableFuture<ManagedLedgerImpl> future = new CompletableFuture<>();
            bookkeeperFactory.get(
                            new EnsemblePlacementPolicyConfig(config.getBookKeeperEnsemblePlacementPolicyClassName(),
                                    config.getBookKeeperEnsemblePlacementPolicyProperties()))
                    .thenAccept(bk -> {
                        final ManagedLedgerImpl newledger = config.getShadowSource() == null
                                ? new ManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name,
                                mlOwnershipChecker)
                                : new ShadowManagedLedgerImpl(this, bk, store, config, scheduledExecutor, name,
                                mlOwnershipChecker);
                        PendingInitializeManagedLedger pendingLedger = new PendingInitializeManagedLedger(newledger);
                        pendingInitializeLedgers.put(name, pendingLedger);
                        newledger.initialize(new ManagedLedgerInitializeLedgerCallback() {
                            @Override
                            public void initializeComplete() {
                                log.info("[{}] Successfully initialize managed ledger", name);
                                pendingInitializeLedgers.remove(name, pendingLedger);
                                future.complete(newledger);

                                // May need to update the cursor position
                                newledger.maybeUpdateCursorBeforeTrimmingConsumedLedger();
                                // May need to trigger offloading
                                if (config.isTriggerOffloadOnTopicLoad()) {
                                    newledger.maybeOffloadInBackground(NULL_OFFLOAD_PROMISE);
                                }
                            }

                            @Override
                            public void initializeFailed(ManagedLedgerException e) {
                                if (config.isCreateIfMissing()) {
                                    log.error("[{}] Failed to initialize managed ledger: {}", name, e.getMessage());
                                }

                                // Clean the map if initialization fails
                                ledgers.remove(name, future);
                                entryCacheManager.removeEntryCache(name);

                                if (pendingInitializeLedgers.remove(name, pendingLedger)) {
                                    pendingLedger.ledger.asyncClose(new CloseCallback() {
                                        @Override
                                        public void closeComplete(Object ctx) {
                                            // no-op
                                        }

                                        @Override
                                        public void closeFailed(ManagedLedgerException exception, Object ctx) {
                                            log.warn("[{}] Failed to a pending initialization managed ledger", name,
                                                    exception);
                                        }
                                    }, null);
                                }

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
                    log.info("[{}] Successfully initialize Read-only managed ledger", managedLedgerName);
                    callback.openReadOnlyManagedLedgerComplete(roManagedLedger, ctx);
                }).exceptionally(e -> {
                    log.error("[{}] Failed to initialize Read-only managed ledger", managedLedgerName, e);
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
        if (ledgers.remove(ledger.getName(), ledgerFuture)) {
            entryCacheManager.removeEntryCache(ledger.getName());
        }
    }

    public CompletableFuture<Void> shutdownAsync() throws ManagedLedgerException {
        if (closed) {
            throw new ManagedLedgerException.ManagedLedgerFactoryClosedException();
        }
        closed = true;

        statsTask.cancel(true);
        flushCursorsTask.cancel(true);
        cacheEvictionExecutor.shutdownNow();

        List<String> ledgerNames = new ArrayList<>(this.ledgers.keySet());
        List<CompletableFuture<Void>> futures = new ArrayList<>(ledgerNames.size());
        int numLedgers = ledgerNames.size();
        log.info("Closing {} ledgers", numLedgers);
        for (String ledgerName : ledgerNames) {
            CompletableFuture<ManagedLedgerImpl> ledgerFuture = ledgers.remove(ledgerName);
            if (ledgerFuture == null) {
                continue;
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            futures.add(future);
            ledgerFuture.whenCompleteAsync((managedLedger, throwable) -> {
                if (throwable != null || managedLedger == null) {
                    future.complete(null);
                    return;
                }
                managedLedger.asyncClose(new AsyncCallbacks.CloseCallback() {
                    @Override
                    public void closeComplete(Object ctx) {
                        future.complete(null);
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        log.warn("[{}] Got exception when closing managed ledger: {}", managedLedger.getName(),
                                exception);
                        future.complete(null);
                    }
                }, null);

            }, scheduledExecutor.chooseThread());
            //close pendingInitializeManagedLedger directly to make sure all callbacks is called.
            PendingInitializeManagedLedger pendingLedger = pendingInitializeLedgers.get(ledgerName);
            if (pendingLedger != null && !ledgerFuture.isDone()) {
                ledgerFuture.completeExceptionally(new ManagedLedgerException.ManagedLedgerFactoryClosedException());
            }
        }
        CompletableFuture<BookKeeper> bookkeeperFuture = isBookkeeperManaged
                ? bookkeeperFactory.get()
                : CompletableFuture.completedFuture(null);
        return bookkeeperFuture
                .thenRun(() -> {
                    log.info("Closing {} ledgers.", ledgers.size());
                    //make sure all callbacks is called.
                    ledgers.forEach(((ledgerName, ledgerFuture) -> {
                        if (!ledgerFuture.isDone()) {
                            ledgerFuture.completeExceptionally(
                                    new ManagedLedgerException.ManagedLedgerFactoryClosedException());
                        } else {
                            ManagedLedgerImpl managedLedger = ledgerFuture.getNow(null);
                            if (managedLedger == null) {
                                return;
                            }
                            try {
                                managedLedger.close();
                            } catch (Throwable throwable) {
                                log.warn("[{}] Got exception when closing managed ledger: {}", managedLedger.getName(),
                                        throwable);
                            }
                        }
                    }));
                }).thenAcceptAsync(__ -> {
                    //wait for tasks in scheduledExecutor executed.
                    openTelemetryManagedCursorStats.close();
                    openTelemetryManagedLedgerStats.close();
                    openTelemetryCacheStats.close();
                    scheduledExecutor.shutdownNow();
                    entryCacheManager.clear();
                });
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
    public ManagedLedgerInfo getManagedLedgerInfo(String name) throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerInfo info = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncGetManagedLedgerInfo(name, new ManagedLedgerInfoCallback() {
            @Override
            public void getInfoComplete(ManagedLedgerInfo info, Object ctx) {
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
                new MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
            @Override
            public void operationComplete(MLDataFormats.ManagedLedgerInfo pbInfo, Stat stat) {
                ManagedLedgerInfo info = new ManagedLedgerInfo();
                info.version = stat.getVersion();
                info.creationDate = DateFormatter.format(stat.getCreationTimestamp());
                info.modificationDate = DateFormatter.format(stat.getModificationTimestamp());

                info.ledgers = new ArrayList<>(pbInfo.getLedgerInfoCount());
                if (pbInfo.hasTerminatedPosition()) {
                    info.terminatedPosition = new PositionInfo();
                    info.terminatedPosition.ledgerId = pbInfo.getTerminatedPosition().getLedgerId();
                    info.terminatedPosition.entryId = pbInfo.getTerminatedPosition().getEntryId();
                }

                if (pbInfo.getPropertiesCount() > 0) {
                    info.properties = new TreeMap();
                    for (int i = 0; i < pbInfo.getPropertiesCount(); i++) {
                        MLDataFormats.KeyValue property = pbInfo.getProperties(i);
                        info.properties.put(property.getKey(), property.getValue());
                    }
                }

                for (int i = 0; i < pbInfo.getLedgerInfoCount(); i++) {
                    MLDataFormats.ManagedLedgerInfo.LedgerInfo pbLedgerInfo = pbInfo.getLedgerInfo(i);
                    LedgerInfo ledgerInfo = new LedgerInfo();
                    ledgerInfo.ledgerId = pbLedgerInfo.getLedgerId();
                    ledgerInfo.entries = pbLedgerInfo.hasEntries() ? pbLedgerInfo.getEntries() : null;
                    ledgerInfo.size = pbLedgerInfo.hasSize() ? pbLedgerInfo.getSize() : null;
                    ledgerInfo.timestamp = pbLedgerInfo.hasTimestamp() ? pbLedgerInfo.getTimestamp() : null;
                    ledgerInfo.isOffloaded = pbLedgerInfo.hasOffloadContext();
                    if (pbLedgerInfo.hasOffloadContext()) {
                        MLDataFormats.OffloadContext offloadContext = pbLedgerInfo.getOffloadContext();
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
                                    new MetaStoreCallback<MLDataFormats.ManagedCursorInfo>() {
                                        @Override
                                        public void operationComplete(ManagedCursorInfo pbCursorInfo, Stat stat) {
                                            CursorInfo cursorInfo = new CursorInfo();
                                            cursorInfo.version = stat.getVersion();
                                            cursorInfo.creationDate = DateFormatter.format(stat.getCreationTimestamp());
                                            cursorInfo.modificationDate = DateFormatter
                                                    .format(stat.getModificationTimestamp());

                                            cursorInfo.cursorsLedgerId = pbCursorInfo.getCursorsLedgerId();

                                            if (pbCursorInfo.hasMarkDeleteLedgerId()) {
                                                cursorInfo.markDelete = new PositionInfo();
                                                cursorInfo.markDelete.ledgerId = pbCursorInfo.getMarkDeleteLedgerId();
                                                cursorInfo.markDelete.entryId = pbCursorInfo.getMarkDeleteEntryId();
                                            }

                                            if (pbCursorInfo.getPropertiesCount() > 0) {
                                                cursorInfo.properties = new TreeMap();
                                                for (int i = 0; i < pbCursorInfo.getPropertiesCount(); i++) {
                                                    LongProperty property = pbCursorInfo.getProperties(i);
                                                    cursorInfo.properties.put(property.getName(), property.getValue());
                                                }
                                            }

                                            if (pbCursorInfo.getIndividualDeletedMessagesCount() > 0) {
                                                cursorInfo.individualDeletedMessages = new ArrayList<>();
                                                for (int i = 0; i < pbCursorInfo
                                                        .getIndividualDeletedMessagesCount(); i++) {
                                                    MessageRange range = pbCursorInfo.getIndividualDeletedMessages(i);
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
            public void getInfoComplete(ManagedLedgerInfo info, Object ctx) {
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

    private void deleteManagedLedgerData(BookKeeper bkc, String managedLedgerName, ManagedLedgerInfo info,
                                         CompletableFuture<ManagedLedgerConfig> mlConfigFuture,
                                         DeleteLedgerCallback callback, Object ctx) {
        final CompletableFuture<Map<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo>>
                ledgerInfosFuture = new CompletableFuture<>();
        store.getManagedLedgerInfo(managedLedgerName, false, null,
                new MetaStoreCallback<>() {
                    @Override
                    public void operationComplete(MLDataFormats.ManagedLedgerInfo mlInfo, Stat stat) {
                        Map<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> infos = new HashMap<>();
                        for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : mlInfo.getLedgerInfoList()) {
                            infos.put(ls.getLedgerId(), ls);
                        }
                        ledgerInfosFuture.complete(infos);
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        log.error("Failed to get managed ledger info for {}", managedLedgerName, e);
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
                            Map<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgerInfos = pair.getRight();

                            if (mlConfig == null || ledgerInfos == null) {
                                return CompletableFuture.completedFuture(null);
                            }

                            MLDataFormats.ManagedLedgerInfo.LedgerInfo ls = ledgerInfos.get(li.ledgerId);

                            if (ls.getOffloadContext().hasUidMsb()) {
                                MLDataFormats.ManagedLedgerInfo.LedgerInfo.Builder newInfoBuilder = ls.toBuilder();
                                newInfoBuilder.getOffloadContextBuilder().setBookkeeperDeleted(true);
                                String driverName = OffloadUtils.getOffloadDriverName(ls,
                                        mlConfig.getLedgerOffloader().getOffloadDriverName());
                                Map<String, String> driverMetadata = OffloadUtils.getOffloadDriverMetadata(ls,
                                        mlConfig.getLedgerOffloader().getOffloadDriverMetadata());
                                OffloadUtils.setOffloadDriverMetadata(newInfoBuilder, driverName, driverMetadata);

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
                                    if (rc == BKException.Code.NoSuchLedgerExistsOnMetadataServerException
                                        || rc == BKException.Code.NoSuchLedgerExistsException) {
                                        log.info("Ledger {} does not exist, ignoring", li.ledgerId);
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
                }).exceptionally(ex -> {
                    callback.deleteLedgerFailed(new ManagedLedgerException(ex), ctx);
                    return null;
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
                            if (rc == BKException.Code.NoSuchLedgerExistsOnMetadataServerException
                                    || rc == BKException.Code.NoSuchLedgerExistsException) {
                                log.info("Ledger {} does not exist, ignoring", cursor.cursorsLedgerId);
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
    public ManagedLedgerFactoryMXBean getCacheStats() {
        return this.mbean;
    }

    public CompletableFuture<BookKeeper> getBookKeeper() {
        return bookkeeperFactory.get();
    }

    @Override
    public void estimateUnloadedTopicBacklog(PersistentOfflineTopicStats offlineTopicStats,
                                                                    TopicName topicName, boolean accurate, Object ctx)
            throws Exception {
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        long numberOfEntries = 0;
        long totalSize = 0;
        BookKeeper.DigestType digestType = (BookKeeper.DigestType) ((List) ctx).get(0);
        byte[] password = (byte[]) ((List) ctx).get(1);
        NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers =
                getManagedLedgersInfo(topicName, accurate, digestType, password);
        for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : ledgers.values()) {
            numberOfEntries += ls.getEntries();
            totalSize += ls.getSize();
            if (accurate) {
                offlineTopicStats.addLedgerDetails(ls.getEntries(), ls.getTimestamp(), ls.getSize(), ls.getLedgerId());
            }
        }
        offlineTopicStats.totalMessages = numberOfEntries;
        offlineTopicStats.storageSize = totalSize;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Total number of entries - {} and size - {}", managedLedgerName, numberOfEntries, totalSize);
        }

        // calculate per cursor message backlog
        calculateCursorBacklogs(topicName, ledgers, offlineTopicStats, accurate, digestType, password);
        offlineTopicStats.statGeneratedAt.setTime(System.currentTimeMillis());
    }

    private NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> getManagedLedgersInfo(
            final TopicName topicName, boolean accurate, BookKeeper.DigestType digestType, byte[] password)
            throws Exception {
        final NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers = new ConcurrentSkipListMap<>();

        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        MetaStore store = getMetaStore();

        final CountDownLatch mlMetaCounter = new CountDownLatch(1);
        store.getManagedLedgerInfo(managedLedgerName, false /* createIfMissing */,
                new MetaStore.MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
                    @Override
                    public void operationComplete(MLDataFormats.ManagedLedgerInfo mlInfo, Stat stat) {
                        for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : mlInfo.getLedgerInfoList()) {
                            ledgers.put(ls.getLedgerId(), ls);
                        }

                        // find no of entries in last ledger
                        if (!ledgers.isEmpty()) {
                            final long id = ledgers.lastKey();
                            AsyncCallback.OpenCallback opencb = (rc, lh, ctx1) -> {
                                if (log.isDebugEnabled()) {
                                    log.debug("[{}] Opened ledger {}: {}", managedLedgerName, id,
                                            BKException.getMessage(rc));
                                }
                                if (rc == BKException.Code.OK) {
                                    MLDataFormats.ManagedLedgerInfo.LedgerInfo info =
                                            MLDataFormats.ManagedLedgerInfo.LedgerInfo
                                                    .newBuilder().setLedgerId(id)
                                                    .setEntries(lh.getLastAddConfirmed() + 1)
                                                    .setSize(lh.getLength()).setTimestamp(System.currentTimeMillis())
                                                    .build();
                                    ledgers.put(id, info);
                                    mlMetaCounter.countDown();
                                } else if (Errors.isNoSuchLedgerExistsException(rc)) {
                                    log.warn("[{}] Ledger not found: {}", managedLedgerName, ledgers.lastKey());
                                    ledgers.remove(ledgers.lastKey());
                                    mlMetaCounter.countDown();
                                } else {
                                    log.error("[{}] Failed to open ledger {}: {}", managedLedgerName, id,
                                            BKException.getMessage(rc));
                                    mlMetaCounter.countDown();
                                }
                            };

                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Opening ledger {}", managedLedgerName, id);
                            }
                            getBookKeeper()
                                    .thenAccept(bk -> {
                                        bk.asyncOpenLedgerNoRecovery(id, digestType, password, opencb, null);
                                    }).exceptionally(ex -> {
                                        log.warn("[{}] Failed to open ledger {}: {}", managedLedgerName, id, ex);
                                        opencb.openComplete(-1, null, null);
                                        mlMetaCounter.countDown();
                                        return null;
                                    });
                        } else {
                            log.warn("[{}] Ledger list empty", managedLedgerName);
                            mlMetaCounter.countDown();
                        }
                    }

                    @Override
                    public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                        log.warn("[{}] Unable to obtain managed ledger metadata - {}", managedLedgerName, e);
                        mlMetaCounter.countDown();
                    }
                });

        if (accurate) {
            // block until however long it takes for operation to complete
            mlMetaCounter.await();
        } else {
            mlMetaCounter.await(META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        return ledgers;
    }

    public void calculateCursorBacklogs(final TopicName topicName,
                                         final NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers,
                                         final PersistentOfflineTopicStats offlineTopicStats, boolean accurate,
                                        BookKeeper.DigestType digestType, byte[] password) throws Exception {
        if (ledgers.isEmpty()) {
            return;
        }
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        MetaStore store = getMetaStore();
        BookKeeper bk = getBookKeeper().get();
        final CountDownLatch allCursorsCounter = new CountDownLatch(1);
        final long errorInReadingCursor = -1;
        final var ledgerRetryMap = new ConcurrentHashMap<String, Long>();

        final MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ledgers.lastEntry().getValue();
        final Position lastLedgerPosition =
                PositionFactory.create(ledgerInfo.getLedgerId(), ledgerInfo.getEntries() - 1);
        if (log.isDebugEnabled()) {
            log.debug("[{}] Last ledger position {}", managedLedgerName, lastLedgerPosition);
        }

        store.getCursors(managedLedgerName, new MetaStore.MetaStoreCallback<List<String>>() {
            @Override
            public void operationComplete(List<String> cursors, Stat v) {
                // Load existing cursors
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Found {} cursors", managedLedgerName, cursors.size());
                }

                if (cursors.isEmpty()) {
                    allCursorsCounter.countDown();
                    return;
                }

                final CountDownLatch cursorCounter = new CountDownLatch(cursors.size());

                for (final String cursorName : cursors) {
                    // determine subscription position from cursor ledger
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Loading cursor {}", managedLedgerName, cursorName);
                    }

                    AsyncCallback.OpenCallback cursorLedgerOpenCb = (rc, lh, ctx1) -> {
                        long ledgerId = lh.getId();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Opened cursor ledger {} for cursor {}. rc={}", managedLedgerName, ledgerId,
                                    cursorName, rc);
                        }
                        if (rc != BKException.Code.OK) {
                            log.warn("[{}] Error opening metadata ledger {} for cursor {}: {}", managedLedgerName,
                                    ledgerId, cursorName, BKException.getMessage(rc));
                            cursorCounter.countDown();
                            return;
                        }
                        long lac = lh.getLastAddConfirmed();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Cursor {} LAC {} read from ledger {}", managedLedgerName, cursorName, lac,
                                    ledgerId);
                        }

                        if (lac == LedgerHandle.INVALID_ENTRY_ID) {
                            // save the ledger id and cursor to retry outside of this call back
                            // since we are trying to read the same cursor ledger, we will block until
                            // this current callback completes, since an attempt to read the entry
                            // will block behind this current operation to complete
                            ledgerRetryMap.put(cursorName, ledgerId);
                            log.info("[{}] Cursor {} LAC {} read from ledger {}", managedLedgerName, cursorName, lac,
                                    ledgerId);
                            cursorCounter.countDown();
                            return;
                        }
                        final long entryId = lac;
                        // read last acked message position for subscription
                        lh.asyncReadEntries(entryId, entryId, new AsyncCallback.ReadCallback() {
                            @Override
                            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq,
                                                     Object ctx) {
                                try {
                                    if (log.isDebugEnabled()) {
                                        log.debug("readComplete rc={} entryId={}", rc, entryId);
                                    }
                                    if (rc != BKException.Code.OK) {
                                        log.warn("[{}] Error reading from metadata ledger {} for cursor {}: {}",
                                                managedLedgerName, ledgerId, cursorName, BKException.getMessage(rc));
                                        // indicate that this cursor should be excluded
                                        offlineTopicStats.addCursorDetails(cursorName, errorInReadingCursor,
                                                lh.getId());
                                    } else {
                                        LedgerEntry entry = seq.nextElement();
                                        MLDataFormats.PositionInfo positionInfo;
                                        try {
                                            positionInfo = MLDataFormats.PositionInfo.parseFrom(entry.getEntry());
                                        } catch (InvalidProtocolBufferException e) {
                                            log.warn(
                                                    "[{}] Error reading position from metadata ledger {} for cursor "
                                                            + "{}: {}", managedLedgerName, ledgerId, cursorName, e);
                                            offlineTopicStats.addCursorDetails(cursorName, errorInReadingCursor,
                                                    lh.getId());
                                            return;
                                        }
                                        final Position lastAckedMessagePosition =
                                                PositionFactory.create(positionInfo.getLedgerId(),
                                                        positionInfo.getEntryId());
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Cursor {} MD {} read last ledger position {}",
                                                    managedLedgerName, cursorName, lastAckedMessagePosition,
                                                    lastLedgerPosition);
                                        }
                                        // calculate cursor backlog
                                        Range<Position> range = Range.openClosed(lastAckedMessagePosition,
                                                lastLedgerPosition);
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Calculating backlog for cursor {} using range {}",
                                                    managedLedgerName, cursorName, range);
                                        }
                                        long cursorBacklog = getNumberOfEntries(range, ledgers);
                                        offlineTopicStats.messageBacklog += cursorBacklog;
                                        offlineTopicStats.addCursorDetails(cursorName, cursorBacklog, lh.getId());
                                    }
                                } finally {
                                    cursorCounter.countDown();
                                }
                            }
                        }, null);

                    }; // end of cursor meta read callback

                    store.asyncGetCursorInfo(managedLedgerName, cursorName,
                            new MetaStore.MetaStoreCallback<MLDataFormats.ManagedCursorInfo>() {
                                @Override
                                public void operationComplete(MLDataFormats.ManagedCursorInfo info,
                                                              Stat stat) {
                                    long cursorLedgerId = info.getCursorsLedgerId();
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Cursor {} meta-data read ledger id {}", managedLedgerName,
                                                cursorName, cursorLedgerId);
                                    }
                                    if (cursorLedgerId != -1) {
                                        bk.asyncOpenLedgerNoRecovery(cursorLedgerId, digestType, password,
                                                cursorLedgerOpenCb, null);
                                    } else {
                                        Position lastAckedMessagePosition = PositionFactory.create(
                                                info.getMarkDeleteLedgerId(), info.getMarkDeleteEntryId());
                                        Range<Position> range = Range.openClosed(lastAckedMessagePosition,
                                                lastLedgerPosition);
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Calculating backlog for cursor {} using range {}",
                                                    managedLedgerName, cursorName, range);
                                        }
                                        long cursorBacklog = getNumberOfEntries(range, ledgers);
                                        offlineTopicStats.messageBacklog += cursorBacklog;
                                        offlineTopicStats.addCursorDetails(cursorName, cursorBacklog, cursorLedgerId);
                                        cursorCounter.countDown();
                                    }

                                }

                                @Override
                                public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                                    log.warn("[{}] Unable to obtain cursor ledger for cursor {}: {}", managedLedgerName,
                                            cursorName, e);
                                    cursorCounter.countDown();
                                }
                            });
                } // for every cursor find backlog
                try {
                    if (accurate) {
                        cursorCounter.await();
                    } else {
                        cursorCounter.await(META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    log.warn("[{}] Error reading subscription positions{}", managedLedgerName, e);
                } finally {
                    allCursorsCounter.countDown();
                }
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                log.warn("[{}] Failed to get the cursors list", managedLedgerName, e);
                allCursorsCounter.countDown();
            }
        });
        if (accurate) {
            allCursorsCounter.await();
        } else {
            allCursorsCounter.await(META_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        // go through ledgers where LAC was -1
        if (accurate && ledgerRetryMap.size() > 0) {
            ledgerRetryMap.forEach((cursorName, ledgerId) -> {
                if (log.isDebugEnabled()) {
                    log.debug("Cursor {} Ledger {} Trying to obtain MD from BkAdmin", cursorName, ledgerId);
                }
                Position lastAckedMessagePosition = tryGetMDPosition(bk, ledgerId, cursorName);
                if (lastAckedMessagePosition == null) {
                    log.warn("[{}] Cursor {} read from ledger {}. Unable to determine cursor position",
                            managedLedgerName, cursorName, ledgerId);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Cursor {} read from ledger using bk admin {}. position {}", managedLedgerName,
                                cursorName, ledgerId, lastAckedMessagePosition);
                    }
                    // calculate cursor backlog
                    Range<Position> range = Range.openClosed(lastAckedMessagePosition, lastLedgerPosition);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Calculating backlog for cursor {} using range {}", managedLedgerName,
                                cursorName, range);
                    }
                    long cursorBacklog = getNumberOfEntries(range, ledgers);
                    offlineTopicStats.messageBacklog += cursorBacklog;
                    offlineTopicStats.addCursorDetails(cursorName, cursorBacklog, ledgerId);
                }
            });
        }
    }

    // need a better way than to duplicate the functionality below from ML
    private long getNumberOfEntries(Range<Position> range,
                                    NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers) {
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
            MLDataFormats.ManagedLedgerInfo.LedgerInfo li = ledgers.get(fromPosition.getLedgerId());
            if (li != null) {
                count += li.getEntries() - (fromPosition.getEntryId() + 1);
                count += fromIncluded ? 1 : 0;
            }

            // 3. Add the whole ledgers entries in between
            for (MLDataFormats.ManagedLedgerInfo.LedgerInfo ls : ledgers
                    .subMap(fromPosition.getLedgerId(), false, toPosition.getLedgerId(), false).values()) {
                count += ls.getEntries();
            }

            return count;
        }
    }


    private Position tryGetMDPosition(BookKeeper bookKeeper, long ledgerId, String cursorName) {
        BookKeeperAdmin bookKeeperAdmin = null;
        long lastEntry = LedgerHandle.INVALID_ENTRY_ID;
        Position lastAckedMessagePosition = null;
        try {
            bookKeeperAdmin = new BookKeeperAdmin(bookKeeper);
            for (LedgerEntry ledgerEntry : bookKeeperAdmin.readEntries(ledgerId, 0, lastEntry)) {
                lastEntry = ledgerEntry.getEntryId();
                if (log.isDebugEnabled()) {
                    log.debug(" Read entry {} from ledger {} for cursor {}", lastEntry, ledgerId, cursorName);
                }
                MLDataFormats.PositionInfo positionInfo = MLDataFormats.PositionInfo.parseFrom(ledgerEntry.getEntry());
                lastAckedMessagePosition =
                        PositionFactory.create(positionInfo.getLedgerId(), positionInfo.getEntryId());
                if (log.isDebugEnabled()) {
                    log.debug("Cursor {} read position {}", cursorName, lastAckedMessagePosition);
                }
            }
        } catch (Exception e) {
            log.warn("Unable to determine LAC for ledgerId {} for cursor {}: {}", ledgerId, cursorName, e);
        } finally {
            if (bookKeeperAdmin != null) {
                try {
                    bookKeeperAdmin.close();
                } catch (Exception e) {
                    log.warn("Unable to close bk admin for ledgerId {} for cursor {}", ledgerId, cursorName, e);
                }
            }

        }
        return lastAckedMessagePosition;
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

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerFactoryImpl.class);
}

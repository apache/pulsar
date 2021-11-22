/**
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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.mledger.ManagedLedgerException.getManagedLedgerException;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
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
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.ManagedLedgerInitializeLedgerCallback;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.LongProperty;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.MessageRange;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.common.util.FutureUtil;
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
    protected final OrderedScheduler scheduledExecutor;

    private final ExecutorService cacheEvictionExecutor;

    protected final ManagedLedgerFactoryMBeanImpl mbean;

    protected final ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, PendingInitializeManagedLedger> pendingInitializeLedgers =
        new ConcurrentHashMap<>();
    private final EntryCacheManager entryCacheManager;

    private long lastStatTimestamp = System.nanoTime();
    private final ScheduledFuture<?> statsTask;
    private final ScheduledFuture<?> flushCursorsTask;

    private final long cacheEvictionTimeThresholdNanos;
    private final MetadataStore metadataStore;

    //indicate whether shutdown() is called.
    private volatile boolean closed;

    /**
     * Keep a flag to indicate whether we're currently connected to the metadata service
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
                true /* isBookkeeperManaged */, config, NullStatsLogger.INSTANCE);
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, BookKeeper bookKeeper)
            throws Exception {
        this(metadataStore, bookKeeper, new ManagedLedgerFactoryConfig());
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore, BookKeeper bookKeeper,
                                    ManagedLedgerFactoryConfig config)
            throws Exception {
        this(metadataStore, (policyConfig) -> bookKeeper, config);
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                    BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                    ManagedLedgerFactoryConfig config)
            throws Exception {
        this(metadataStore, bookKeeperGroupFactory, false /* isBookkeeperManaged */,
                config, NullStatsLogger.INSTANCE);
    }

    public ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                    BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                    ManagedLedgerFactoryConfig config, StatsLogger statsLogger)
            throws Exception {
        this(metadataStore, bookKeeperGroupFactory, false /* isBookkeeperManaged */,
                config, statsLogger);
    }

    private ManagedLedgerFactoryImpl(MetadataStoreExtended metadataStore,
                                     BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                     boolean isBookkeeperManaged,
                                     ManagedLedgerFactoryConfig config, StatsLogger statsLogger) throws Exception {
        scheduledExecutor = OrderedScheduler.newSchedulerBuilder()
                .numThreads(config.getNumManagedLedgerSchedulerThreads())
                .statsLogger(statsLogger)
                .traceTaskExecution(config.isTraceTaskExecution())
                .name("bookkeeper-ml-scheduler")
                .build();
        cacheEvictionExecutor = Executors
                .newSingleThreadExecutor(new DefaultThreadFactory("bookkeeper-ml-cache-eviction"));
        this.metadataServiceAvailable = true;
        this.bookkeeperFactory = bookKeeperGroupFactory;
        this.isBookkeeperManaged = isBookkeeperManaged;
        this.metadataStore = metadataStore;
        this.store = new MetaStoreImpl(metadataStore, scheduledExecutor, config.getManagedLedgerInfoCompressionType());
        this.config = config;
        this.mbean = new ManagedLedgerFactoryMBeanImpl(this);
        this.entryCacheManager = new EntryCacheManager(this);
        this.statsTask = scheduledExecutor.scheduleAtFixedRate(catchingAndLoggingThrowables(this::refreshStats),
                0, config.getStatsPeriodSeconds(), TimeUnit.SECONDS);
        this.flushCursorsTask = scheduledExecutor.scheduleAtFixedRate(catchingAndLoggingThrowables(this::flushCursors),
                config.getCursorPositionFlushSeconds(), config.getCursorPositionFlushSeconds(), TimeUnit.SECONDS);


        this.cacheEvictionTimeThresholdNanos = TimeUnit.MILLISECONDS
                .toNanos(config.getCacheEvictionTimeThresholdMillis());


        cacheEvictionExecutor.execute(this::cacheEvictionTask);
        closed = false;

        metadataStore.registerSessionListener(this::handleMetadataStoreNotification);
    }

    static class DefaultBkFactory implements BookkeeperFactoryForCustomEnsemblePlacementPolicy {

        private final BookKeeper bkClient;

        public DefaultBkFactory(ClientConfiguration bkClientConfiguration)
                throws InterruptedException, BKException, IOException {
            bkClient = new BookKeeper(bkClientConfiguration);
        }

        @Override
        public BookKeeper get(EnsemblePlacementPolicyConfig policy) {
            return bkClient;
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

    private void cacheEvictionTask() {
        double evictionFrequency = Math.max(Math.min(config.getCacheEvictionFrequency(), 1000.0), 0.001);
        long waitTimeMillis = (long) (1000 / evictionFrequency);

        while (true) {
            try {
                doCacheEviction();

                Thread.sleep(waitTimeMillis);
            } catch (InterruptedException e) {
                // Factory is shutting down
                return;
            } catch (Throwable t) {
                log.warn("Exception while performing cache eviction: {}", t.getMessage(), t);
            }
        }
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
    public Map<String, ManagedLedgerImpl> getManagedLedgers() {
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
            Supplier<Boolean> mlOwnershipChecker, final Object ctx) {
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
                    if (l.getState() == State.Fenced || l.getState() == State.Closed) {
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
            final ManagedLedgerImpl newledger = new ManagedLedgerImpl(this,
                    bookkeeperFactory.get(
                            new EnsemblePlacementPolicyConfig(config.getBookKeeperEnsemblePlacementPolicyClassName(),
                                    config.getBookKeeperEnsemblePlacementPolicyProperties())),
                    store, config, scheduledExecutor, name, mlOwnershipChecker);
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
                }

                @Override
                public void initializeFailed(ManagedLedgerException e) {
                    if (config.isCreateIfMissing()) {
                        log.error("[{}] Failed to initialize managed ledger: {}", name, e.getMessage());
                    }

                    // Clean the map if initialization fails
                    ledgers.remove(name, future);

                    if (pendingInitializeLedgers.remove(name, pendingLedger)) {
                        pendingLedger.ledger.asyncClose(new CloseCallback() {
                            @Override
                            public void closeComplete(Object ctx) {
                                // no-op
                            }

                            @Override
                            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                                log.warn("[{}] Failed to a pending initialization managed ledger", name, exception);
                            }
                        }, null);
                    }

                    future.completeExceptionally(e);
                }
            }, null);
            return future;
        }).thenAccept(ml -> callback.openLedgerComplete(ml, ctx)).exceptionally(exception -> {
            callback.openLedgerFailed((ManagedLedgerException) exception.getCause(), ctx);
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
        checkArgument(startPosition instanceof PositionImpl);
        ReadOnlyManagedLedgerImpl roManagedLedger = new ReadOnlyManagedLedgerImpl(this,
                bookkeeperFactory
                        .get(new EnsemblePlacementPolicyConfig(config.getBookKeeperEnsemblePlacementPolicyClassName(),
                                config.getBookKeeperEnsemblePlacementPolicyProperties())),
                store, config, scheduledExecutor, managedLedgerName);

        roManagedLedger.initializeAndCreateCursor((PositionImpl) startPosition)
                .thenAccept(roCursor -> callback.openReadOnlyCursorComplete(roCursor, ctx))
                .exceptionally(ex -> {
            Throwable t = ex;
            if (t instanceof CompletionException) {
                t = ex.getCause();
            }

            if (t instanceof ManagedLedgerException) {
                callback.openReadOnlyCursorFailed((ManagedLedgerException) t, ctx);
            } else {
                callback.openReadOnlyCursorFailed(new ManagedLedgerException(t), ctx);
            }

            return null;
        });
    }

    void close(ManagedLedger ledger) {
        // Remove the ledger from the internal factory cache
        ledgers.remove(ledger.getName());
        entryCacheManager.removeEntryCache(ledger.getName());
    }

    public CompletableFuture<Void> shutdownAsync() throws ManagedLedgerException {
        if (closed) {
            throw new ManagedLedgerException.ManagedLedgerFactoryClosedException();
        }
        closed = true;

        statsTask.cancel(true);
        flushCursorsTask.cancel(true);

        List<String> ledgerNames = new ArrayList<>(this.ledgers.keySet());
        List<CompletableFuture<Void>> futures = new ArrayList<>(ledgerNames.size());
        int numLedgers = ledgerNames.size();
        log.info("Closing {} ledgers", numLedgers);
        for (String ledgerName : ledgerNames) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            futures.add(future);
            CompletableFuture<ManagedLedgerImpl> ledgerFuture = ledgers.remove(ledgerName);
            if (ledgerFuture == null) {
                future.complete(null);
                continue;
            }
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
        CompletableFuture<Void> bookkeeperFuture = new CompletableFuture<>();
        futures.add(bookkeeperFuture);
        futures.add(CompletableFuture.runAsync(() -> {
            if (isBookkeeperManaged) {
                try {
                    BookKeeper bookkeeper = bookkeeperFactory.get();
                    if (bookkeeper != null) {
                        bookkeeper.close();
                    }
                    bookkeeperFuture.complete(null);
                } catch (Throwable throwable) {
                    bookkeeperFuture.completeExceptionally(throwable);
                }
            } else {
                bookkeeperFuture.complete(null);
            }
            //wait for tasks in scheduledExecutor executed.
            scheduledExecutor.shutdown();

            if (!ledgers.isEmpty()) {
                log.info("Force closing {} ledgers.", ledgers.size());
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
            }
        }));
        cacheEvictionExecutor.shutdownNow();
        entryCacheManager.clear();
        return FutureUtil.waitForAll(futures);
    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {
        if (closed) {
            throw new ManagedLedgerException.ManagedLedgerFactoryClosedException();
        }
        closed = true;

        statsTask.cancel(true);
        flushCursorsTask.cancel(true);

        // take a snapshot of ledgers currently in the map to prevent race conditions
        List<CompletableFuture<ManagedLedgerImpl>> ledgers = new ArrayList<>(this.ledgers.values());
        int numLedgers = ledgers.size();
        final CountDownLatch latch = new CountDownLatch(numLedgers);
        log.info("Closing {} ledgers", numLedgers);

        for (CompletableFuture<ManagedLedgerImpl> ledgerFuture : ledgers) {
            ManagedLedgerImpl ledger = ledgerFuture.getNow(null);
            if (ledger == null) {
                latch.countDown();
                continue;
            }

            ledger.asyncClose(new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    latch.countDown();
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    log.warn("[{}] Got exception when closing managed ledger: {}", ledger.getName(), exception);
                    latch.countDown();
                }
            }, null);
        }

        latch.await();
        log.info("{} ledgers closed", numLedgers);

        if (isBookkeeperManaged) {
            try {
                BookKeeper bookkeeper = bookkeeperFactory.get();
                if (bookkeeper != null) {
                    bookkeeper.close();
                }
            } catch (BKException e) {
                throw new ManagedLedgerException(e);
            }
        }

        scheduledExecutor.shutdownNow();
        cacheEvictionExecutor.shutdownNow();

        entryCacheManager.clear();
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
                    info.properties = Maps.newTreeMap();
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
                    ledgerInfo.isOffloaded = pbLedgerInfo.hasOffloadContext();
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
                                                cursorInfo.properties = Maps.newTreeMap();
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
        class Result {
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncDelete(name, new DeleteLedgerCallback() {
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
        CompletableFuture<ManagedLedgerImpl> future = ledgers.get(name);
        if (future == null) {
            // Managed ledger does not exist and we're not currently trying to open it
            deleteManagedLedger(name, callback, ctx);
        } else {
            future.thenAccept(ml -> {
                // If it's open, delete in the normal way
                ml.asyncDelete(callback, ctx);
            }).exceptionally(ex -> {
                // If it's failing to get open, just delete from metadata
                return null;
            });
        }
    }

    /**
     * Delete all managed ledger resources and metadata.
     */
    void deleteManagedLedger(String managedLedgerName, DeleteLedgerCallback callback, Object ctx) {
        // Read the managed ledger metadata from store
        asyncGetManagedLedgerInfo(managedLedgerName, new ManagedLedgerInfoCallback() {
            @Override
            public void getInfoComplete(ManagedLedgerInfo info, Object ctx) {
                BookKeeper bkc = getBookKeeper();

                // First delete all cursors resources
                List<CompletableFuture<Void>> futures = info.cursors.entrySet().stream()
                        .map(e -> deleteCursor(bkc, managedLedgerName, e.getKey(), e.getValue()))
                        .collect(Collectors.toList());
                Futures.waitForAll(futures).thenRun(() -> {
                    deleteManagedLedgerData(bkc, managedLedgerName, info, callback, ctx);
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
            DeleteLedgerCallback callback, Object ctx) {
        Futures.waitForAll(info.ledgers.stream()
                .filter(li -> !li.isOffloaded)
                .map(li -> bkc.newDeleteLedgerOp().withLedgerId(li.ledgerId).execute())
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
            cursorLedgerDeleteFuture = bkc.newDeleteLedgerOp().withLedgerId(cursor.cursorsLedgerId).execute();
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

    public MetaStore getMetaStore() {
        return store;
    }

    public ManagedLedgerFactoryConfig getConfig() {
        return config;
    }

    public EntryCacheManager getEntryCacheManager() {
        return entryCacheManager;
    }

    public ManagedLedgerFactoryMXBean getCacheStats() {
        return this.mbean;
    }

    public BookKeeper getBookKeeper() {
        return bookkeeperFactory.get();
    }

    /**
     * Factory to create Bookkeeper-client for a given ensemblePlacementPolicy.
     *
     */
    public interface BookkeeperFactoryForCustomEnsemblePlacementPolicy {
        default BookKeeper get() {
            return get(null);
        }

        /**
         * Returns Bk-Client for a given ensemblePlacementPolicyMetadata. It returns default bK-client if
         * ensemblePlacementPolicyMetadata is null.
         *
         * @param ensemblePlacementPolicyMetadata
         * @return
         */
        BookKeeper get(EnsemblePlacementPolicyConfig ensemblePlacementPolicyMetadata);
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerFactoryImpl.class);
}

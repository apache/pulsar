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

import com.google.common.base.Objects;
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

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.common.util.OrderedExecutor;
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
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.common.util.DateFormatter;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.zookeeper.ZKMetadataStore;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerFactoryImpl implements ManagedLedgerFactory {
    private final MetaStore store;
    private final BookkeeperFactoryForCustomEnsemblePlacementPolicy bookkeeperFactory;
    private final boolean isBookkeeperManaged;
    private final ZooKeeper zookeeper;
    private final ManagedLedgerFactoryConfig config;
    protected final OrderedScheduler scheduledExecutor;
    private final OrderedExecutor orderedExecutor;

    private final ExecutorService cacheEvictionExecutor;

    protected final ManagedLedgerFactoryMBeanImpl mbean;

    protected final ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<String, PendingInitializeManagedLedger> pendingInitializeLedgers =
        new ConcurrentHashMap<>();
    private final EntryCacheManager entryCacheManager;

    private long lastStatTimestamp = System.nanoTime();
    private final ScheduledFuture<?> statsTask;

    private final long cacheEvictionTimeThresholdNanos;
    private final MetadataStore metadataStore;

    private static final int StatsPeriodSeconds = 60;

    private static class PendingInitializeManagedLedger {

        private final ManagedLedgerImpl ledger;
        private final long createTimeMs;

        PendingInitializeManagedLedger(ManagedLedgerImpl ledger) {
            this.ledger = ledger;
            this.createTimeMs = System.currentTimeMillis();
        }

    }

    public ManagedLedgerFactoryImpl(ClientConfiguration bkClientConfiguration, String zkConnection) throws Exception {
        this(bkClientConfiguration, zkConnection, new ManagedLedgerFactoryConfig());
    }

    @SuppressWarnings("deprecation")
    public ManagedLedgerFactoryImpl(ClientConfiguration bkClientConfiguration, ManagedLedgerFactoryConfig config)
            throws Exception {
        this(ZooKeeperClient.newBuilder()
                .connectString(bkClientConfiguration.getZkServers())
                .sessionTimeoutMs(bkClientConfiguration.getZkTimeout())
                .build(), bkClientConfiguration, config);
    }

    private ManagedLedgerFactoryImpl(ZooKeeper zkc, ClientConfiguration bkClientConfiguration,
            ManagedLedgerFactoryConfig config) throws Exception {
        this(new DefaultBkFactory(bkClientConfiguration, zkc), true /* isBookkeeperManaged */,
                zkc, config, NullStatsLogger.INSTANCE);
    }

    private ManagedLedgerFactoryImpl(ClientConfiguration clientConfiguration, String zkConnection, ManagedLedgerFactoryConfig config) throws Exception {
        this(new DefaultBkFactory(clientConfiguration),
            true,
            ZooKeeperClient.newBuilder()
                .connectString(zkConnection)
                .sessionTimeoutMs(clientConfiguration.getZkTimeout()).build(), config, NullStatsLogger.INSTANCE);
    }

    public ManagedLedgerFactoryImpl(BookKeeper bookKeeper, ZooKeeper zooKeeper) throws Exception {
        this((policyConfig) -> bookKeeper, zooKeeper, new ManagedLedgerFactoryConfig());
    }

    public ManagedLedgerFactoryImpl(BookKeeper bookKeeper, ZooKeeper zooKeeper, ManagedLedgerFactoryConfig config)
            throws Exception {
        this((policyConfig) -> bookKeeper, false /* isBookkeeperManaged */,
                zooKeeper, config, NullStatsLogger.INSTANCE);
    }

    public ManagedLedgerFactoryImpl(BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                    ZooKeeper zooKeeper, ManagedLedgerFactoryConfig config)
            throws Exception {
        this(bookKeeperGroupFactory, false /* isBookkeeperManaged */, zooKeeper, config, NullStatsLogger.INSTANCE);
    }

    public ManagedLedgerFactoryImpl(BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                    ZooKeeper zooKeeper, ManagedLedgerFactoryConfig config, StatsLogger statsLogger)
            throws Exception {
        this(bookKeeperGroupFactory, false /* isBookkeeperManaged */, zooKeeper, config, statsLogger);
    }

    private ManagedLedgerFactoryImpl(BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
                                     boolean isBookkeeperManaged, ZooKeeper zooKeeper,
                                     ManagedLedgerFactoryConfig config, StatsLogger statsLogger) throws Exception {
        scheduledExecutor = OrderedScheduler.newSchedulerBuilder()
                .numThreads(config.getNumManagedLedgerSchedulerThreads())
                .statsLogger(statsLogger)
                .traceTaskExecution(config.isTraceTaskExecution())
                .name("bookkeeper-ml-scheduler")
                .build();
        orderedExecutor = OrderedExecutor.newBuilder()
                .numThreads(config.getNumManagedLedgerWorkerThreads())
                .statsLogger(statsLogger)
                .traceTaskExecution(config.isTraceTaskExecution())
                .name("bookkeeper-ml-workers")
                .build();
        cacheEvictionExecutor = Executors
                .newSingleThreadExecutor(new DefaultThreadFactory("bookkeeper-ml-cache-eviction"));

        this.bookkeeperFactory = bookKeeperGroupFactory;
        this.isBookkeeperManaged = isBookkeeperManaged;
        this.zookeeper = isBookkeeperManaged ? zooKeeper : null;
        this.metadataStore = new ZKMetadataStore(zooKeeper);
        this.store = new MetaStoreImpl(metadataStore, orderedExecutor);
        this.config = config;
        this.mbean = new ManagedLedgerFactoryMBeanImpl(this);
        this.entryCacheManager = new EntryCacheManager(this);
        this.statsTask = scheduledExecutor.scheduleAtFixedRate(this::refreshStats, 0, StatsPeriodSeconds, TimeUnit.SECONDS);


        this.cacheEvictionTimeThresholdNanos = TimeUnit.MILLISECONDS
                .toNanos(config.getCacheEvictionTimeThresholdMillis());


        cacheEvictionExecutor.execute(this::cacheEvictionTask);
    }

    static class DefaultBkFactory implements BookkeeperFactoryForCustomEnsemblePlacementPolicy {

        private final BookKeeper bkClient;

        public DefaultBkFactory(ClientConfiguration bkClientConfiguration, ZooKeeper zkc)
                throws BKException, IOException, InterruptedException {
            bkClient = new BookKeeper(bkClientConfiguration, zkc);
        }

        public DefaultBkFactory(ClientConfiguration bkClientConfiguration) throws InterruptedException, BKException, IOException {
            bkClient = new BookKeeper(bkClientConfiguration);
        }

        @Override
        public BookKeeper get(EnsemblePlacementPolicyConfig policy) {
            return bkClient;
        }
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

        // If the ledger state is bad, remove it from the map.
        CompletableFuture<ManagedLedgerImpl> existingFuture = ledgers.get(name);
        if (existingFuture != null) {
            if (existingFuture.isDone()) {
                try {
                    ManagedLedgerImpl l = existingFuture.get();
                    if (l.getState().equals(State.Fenced.toString()) || l.getState().equals(State.Closed.toString())) {
                        // Managed ledger is in unusable state. Recreate it.
                        log.warn("[{}] Attempted to open ledger in {} state. Removing from the map to recreate it", name,
                            l.getState());
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
                    store, config, scheduledExecutor,
                    orderedExecutor, name, mlOwnershipChecker);
            PendingInitializeManagedLedger pendingLedger = new PendingInitializeManagedLedger(newledger);
            pendingInitializeLedgers.put(name, pendingLedger);
            newledger.initialize(new ManagedLedgerInitializeLedgerCallback() {
                @Override
                public void initializeComplete() {
                    log.info("[{}] Successfully initialize managed ledger", name);
                    pendingInitializeLedgers.remove(name, pendingLedger);
                    future.complete(newledger);
                }

                @Override
                public void initializeFailed(ManagedLedgerException e) {
                    log.error("[{}] Failed to initialize managed ledger: {}", name, e.getMessage());

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
    public ReadOnlyCursor openReadOnlyCursor(String managedLedgerName, Position startPosition, ManagedLedgerConfig config)
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
        checkArgument(startPosition instanceof PositionImpl);
        ReadOnlyManagedLedgerImpl roManagedLedger = new ReadOnlyManagedLedgerImpl(this,
                bookkeeperFactory
                        .get(new EnsemblePlacementPolicyConfig(config.getBookKeeperEnsemblePlacementPolicyClassName(),
                                config.getBookKeeperEnsemblePlacementPolicyProperties())),
                store, config, scheduledExecutor, orderedExecutor, managedLedgerName);

        roManagedLedger.initializeAndCreateCursor((PositionImpl) startPosition).thenAccept(roCursor -> callback.openReadOnlyCursorComplete(roCursor, ctx)).exceptionally(ex -> {
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

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {
        statsTask.cancel(true);

        int numLedgers = ledgers.size();
        final CountDownLatch latch = new CountDownLatch(numLedgers);
        log.info("Closing {} ledgers", numLedgers);

        for (CompletableFuture<ManagedLedgerImpl> ledgerFuture : ledgers.values()) {
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

        if (zookeeper != null) {
            zookeeper.close();
        }

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
        orderedExecutor.shutdownNow();
        cacheEvictionExecutor.shutdownNow();

        entryCacheManager.clear();
        try {
            metadataStore.close();
        } catch (Exception e) {
            throw new ManagedLedgerException(e);
        }
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
     * Delete all managed ledger resources and metadata
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

    private CompletableFuture<Void> deleteCursor(BookKeeper bkc, String managedLedgerName, String cursorName, CursorInfo cursor) {
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
     * Factory to create Bookkeeper-client for a given ensemblePlacementPolicy
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

    public static class EnsemblePlacementPolicyConfig {
        private final Class<? extends EnsemblePlacementPolicy> policyClass;
        private final Map<String, Object> properties;

        public EnsemblePlacementPolicyConfig(Class<? extends EnsemblePlacementPolicy> policyClass,
                Map<String, Object> properties) {
            super();
            this.policyClass = policyClass;
            this.properties = properties;
        }

        public Class<? extends EnsemblePlacementPolicy> getPolicyClass() {
            return policyClass;
        }

        public Map<String, Object> getProperties() {
            return properties;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(policyClass != null ? policyClass.getName() : "", properties);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof EnsemblePlacementPolicyConfig) {
                EnsemblePlacementPolicyConfig other = (EnsemblePlacementPolicyConfig) obj;
                return Objects.equal(this.policyClass == null ? null : this.policyClass.getName(),
                        other.policyClass == null ? null : other.policyClass.getName())
                        && Objects.equal(this.properties, other.properties);
            }
            return false;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerFactoryImpl.class);
}

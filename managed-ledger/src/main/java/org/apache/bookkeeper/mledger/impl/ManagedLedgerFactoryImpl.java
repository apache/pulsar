/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.ManagedLedgerInitializeLedgerCallback;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import io.netty.util.concurrent.DefaultThreadFactory;

public class ManagedLedgerFactoryImpl implements ManagedLedgerFactory {
    private final MetaStore store;
    private final BookKeeper bookKeeper;
    private final boolean isBookkeeperManaged;
    private final ZooKeeper zookeeper;
    private final ManagedLedgerFactoryConfig config;
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(16,
            new DefaultThreadFactory("bookkeeper-ml"));
    private final OrderedSafeExecutor orderedExecutor = new OrderedSafeExecutor(16, "bookkeeper-ml-workers");

    protected final ManagedLedgerFactoryMBeanImpl mbean;

    protected final ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = new ConcurrentHashMap<>();
    private final EntryCacheManager entryCacheManager;

    private long lastStatTimestamp = System.nanoTime();
    private final ScheduledFuture<?> statsTask;
    private static final int StatsPeriodSeconds = 60;

    public ManagedLedgerFactoryImpl(ClientConfiguration bkClientConfiguration) throws Exception {
        this(bkClientConfiguration, new ManagedLedgerFactoryConfig());
    }

    public ManagedLedgerFactoryImpl(ClientConfiguration bkClientConfiguration, ManagedLedgerFactoryConfig config)
            throws Exception {
        final CountDownLatch counter = new CountDownLatch(1);
        final String zookeeperQuorum = checkNotNull(bkClientConfiguration.getZkServers());

        zookeeper = new ZooKeeper(zookeeperQuorum, bkClientConfiguration.getZkTimeout(), event -> {
            if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                log.info("Connected to zookeeper");
                counter.countDown();
            } else {
                log.error("Error connecting to zookeeper {}", event);
            }
        });

        if (!counter.await(bkClientConfiguration.getZkTimeout(), TimeUnit.MILLISECONDS)
                || zookeeper.getState() != States.CONNECTED) {
            throw new ManagedLedgerException("Error connecting to ZooKeeper at '" + zookeeperQuorum + "'");
        }

        this.bookKeeper = new BookKeeper(bkClientConfiguration, zookeeper);
        this.isBookkeeperManaged = true;

        this.store = new MetaStoreImplZookeeper(zookeeper, orderedExecutor);

        this.config = config;
        this.mbean = new ManagedLedgerFactoryMBeanImpl(this);
        this.entryCacheManager = new EntryCacheManager(this);
        this.statsTask = executor.scheduleAtFixedRate(() -> refreshStats(), 0, StatsPeriodSeconds, TimeUnit.SECONDS);
    }

    public ManagedLedgerFactoryImpl(BookKeeper bookKeeper, ZooKeeper zooKeeper) throws Exception {
        this(bookKeeper, zooKeeper, new ManagedLedgerFactoryConfig());
    }

    public ManagedLedgerFactoryImpl(BookKeeper bookKeeper, ZooKeeper zooKeeper, ManagedLedgerFactoryConfig config)
            throws Exception {
        this.bookKeeper = bookKeeper;
        this.isBookkeeperManaged = false;
        this.zookeeper = null;
        this.store = new MetaStoreImplZookeeper(zooKeeper, orderedExecutor);
        this.config = config;
        this.mbean = new ManagedLedgerFactoryMBeanImpl(this);
        this.entryCacheManager = new EntryCacheManager(this);
        this.statsTask = executor.scheduleAtFixedRate(() -> refreshStats(), 0, StatsPeriodSeconds, TimeUnit.SECONDS);
    }

    private synchronized void refreshStats() {
        long now = System.nanoTime();
        long period = now - lastStatTimestamp;

        mbean.refreshStats(period, TimeUnit.NANOSECONDS);
        ledgers.values().forEach(mlfuture -> {
            ManagedLedgerImpl ml = mlfuture.getNow(null);
            if (ml != null) {
                ml.mbean.refreshStats(period, TimeUnit.NANOSECONDS);
            }
        });

        lastStatTimestamp = now;
    }

    /**
     * Helper for getting stats
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
        }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.l;
    }

    @Override
    public void asyncOpen(String name, OpenLedgerCallback callback, Object ctx) {
        asyncOpen(name, new ManagedLedgerConfig(), callback, ctx);
    }

    @Override
    public void asyncOpen(final String name, final ManagedLedgerConfig config, final OpenLedgerCallback callback,
            final Object ctx) {

        // If the ledger state is bad, remove it from the map.
        CompletableFuture<ManagedLedgerImpl> existingFuture = ledgers.get(name);
        if (existingFuture != null && existingFuture.isDone()) {
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
        }

        // Ensure only one managed ledger is created and initialized
        ledgers.computeIfAbsent(name, (mlName) -> {
            // Create the managed ledger
            CompletableFuture<ManagedLedgerImpl> future = new CompletableFuture<>();
            final ManagedLedgerImpl newledger = new ManagedLedgerImpl(this, bookKeeper, store, config, executor,
                    orderedExecutor, name);
            newledger.initialize(new ManagedLedgerInitializeLedgerCallback() {
                @Override
                public void initializeComplete() {
                    future.complete(newledger);
                }

                @Override
                public void initializeFailed(ManagedLedgerException e) {
                    // Clean the map if initialization fails
                    ledgers.remove(name, future);
                    future.completeExceptionally(e);
                }
            }, null);
            return future;
        }).thenAccept(ml -> {
            callback.openLedgerComplete(ml, ctx);
        }).exceptionally(exception -> {
            callback.openLedgerFailed((ManagedLedgerException) exception.getCause(), ctx);
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
                bookKeeper.close();
            } catch (BKException e) {
                throw new ManagedLedgerException(e);
            }
        }

        executor.shutdown();
        orderedExecutor.shutdown();

        entryCacheManager.clear();
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
        return bookKeeper;
    }

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerFactoryImpl.class);
}

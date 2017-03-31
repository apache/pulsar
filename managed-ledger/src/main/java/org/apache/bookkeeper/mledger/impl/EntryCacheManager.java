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

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

public class EntryCacheManager {

    private final long maxSize;
    private final long evictionTriggerThreshold;
    private final double cacheEvictionWatermak;
    private final AtomicLong currentSize = new AtomicLong(0);
    private final ConcurrentMap<String, EntryCache> caches = Maps.newConcurrentMap();
    private final EntryCacheEvictionPolicy evictionPolicy;

    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);

    private final ManagedLedgerFactoryImpl mlFactory;
    protected final ManagedLedgerFactoryMBeanImpl mlFactoryMBean;

    protected static final double MB = 1024 * 1024;

    private static final double evictionTriggerThresholdPercent = 0.98;

    /**
     *
     */
    public EntryCacheManager(ManagedLedgerFactoryImpl factory) {
        this.maxSize = factory.getConfig().getMaxCacheSize();
        this.evictionTriggerThreshold = (long) (maxSize * evictionTriggerThresholdPercent);
        this.cacheEvictionWatermak = factory.getConfig().getCacheEvictionWatermark();
        this.evictionPolicy = new EntryCacheDefaultEvictionPolicy();
        this.mlFactory = factory;
        this.mlFactoryMBean = factory.mbean;

        log.info("Initialized managed-ledger entry cache of {} Mb", maxSize / MB);
    }

    public EntryCache getEntryCache(ManagedLedgerImpl ml) {
        if (maxSize == 0) {
            // Cache is disabled
            return new EntryCacheDisabled(ml);
        }

        EntryCache newEntryCache = new EntryCacheImpl(this, ml);
        EntryCache currentEntryCache = caches.putIfAbsent(ml.getName(), newEntryCache);
        if (currentEntryCache != null) {
            return currentEntryCache;
        } else {
            return newEntryCache;
        }
    }

    void removeEntryCache(String name) {
        EntryCache entryCache = caches.remove(name);
        if (entryCache == null) {
            return;
        }

        long size = entryCache.getSize();
        entryCache.clear();

        if (log.isDebugEnabled()) {
            log.debug("Removed cache for {} - Size: {} -- Current Size: {}", name, size / MB, currentSize.get() / MB);
        }
    }

    boolean hasSpaceInCache() {
        long currentSize = this.currentSize.get();

        // Trigger a single eviction in background. While the eviction is running we stop inserting entries in the cache
        if (currentSize > evictionTriggerThreshold && evictionInProgress.compareAndSet(false, true)) {
            mlFactory.executor.execute(safeRun(() -> {
                // Trigger a new cache eviction cycle to bring the used memory below the cacheEvictionWatermark
                // percentage limit
                long sizeToEvict = currentSize - (long) (maxSize * cacheEvictionWatermak);
                long startTime = System.nanoTime();
                log.info("Triggering cache eviction. total size: {} Mb -- Need to discard: {} Mb", currentSize / MB,
                        sizeToEvict / MB);

                try {
                    evictionPolicy.doEviction(Lists.newArrayList(caches.values()), sizeToEvict);

                    long endTime = System.nanoTime();
                    double durationMs = TimeUnit.NANOSECONDS.toMicros(endTime - startTime) / 1000.0;

                    log.info("Eviction completed. Removed {} Mb in {} ms", (currentSize - this.currentSize.get()) / MB,
                            durationMs);
                } finally {
                    mlFactoryMBean.recordCacheEviction();
                    evictionInProgress.set(false);
                }
            }));
        }

        return currentSize < maxSize;
    }

    void entryAdded(long size) {
        currentSize.addAndGet(size);
    }

    void entriesRemoved(long size) {
        currentSize.addAndGet(-size);
    }

    public long getSize() {
        return currentSize.get();
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void clear() {
        caches.values().forEach(cache -> cache.clear());
    }

    protected class EntryCacheDisabled implements EntryCache {
        private final ManagedLedgerImpl ml;

        public EntryCacheDisabled(ManagedLedgerImpl ml) {
            this.ml = ml;
        }

        @Override
        public String getName() {
            return ml.getName();
        }

        @Override
        public boolean insert(EntryImpl entry) {
            return false;
        }

        @Override
        public void invalidateEntries(PositionImpl lastPosition) {
        }

        @Override
        public void invalidateAllEntries(long ledgerId) {
        }

        @Override
        public void clear() {
        }

        @Override
        public Pair<Integer, Long> evictEntries(long sizeToFree) {
            return Pair.create(0, (long) 0);
        }

        @Override
        public void asyncReadEntry(LedgerHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
                final ReadEntriesCallback callback, Object ctx) {
            lh.asyncReadEntries(firstEntry, lastEntry, new ReadCallback() {
                public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object bkctx) {
                    if (rc != BKException.Code.OK) {
                        callback.readEntriesFailed(new ManagedLedgerException(BKException.create(rc)), ctx);
                        return;
                    }

                    List<Entry> entries = Lists.newArrayList();
                    long totalSize = 0;
                    while (seq.hasMoreElements()) {
                        // Insert the entries at the end of the list (they will be unsorted for now)
                        LedgerEntry ledgerEntry = seq.nextElement();
                        EntryImpl entry = EntryImpl.create(ledgerEntry);
                        ledgerEntry.getEntryBuffer().release();

                        entries.add(entry);
                        totalSize += entry.getLength();
                    }

                    mlFactoryMBean.recordCacheMiss(entries.size(), totalSize);
                    ml.mbean.addReadEntriesSample(entries.size(), totalSize);

                    callback.readEntriesComplete(entries, null);
                }
            }, null);
        }

        @Override
        public void asyncReadEntry(LedgerHandle lh, PositionImpl position, AsyncCallbacks.ReadEntryCallback callback,
                Object ctx) {
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public int compareTo(EntryCache other) {
            return Longs.compare(getSize(), other.getSize());
        }

    }

    private static final Logger log = LoggerFactory.getLogger(EntryCacheManager.class);
}

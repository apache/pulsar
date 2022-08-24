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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:javadoctype")
public class RangeEntryCacheManagerImpl implements EntryCacheManager {

    private volatile long maxSize;
    private volatile long evictionTriggerThreshold;
    private volatile double cacheEvictionWatermark;
    private final AtomicLong currentSize = new AtomicLong(0);
    private final ConcurrentMap<String, EntryCache> caches = Maps.newConcurrentMap();
    private final EntryCacheEvictionPolicy evictionPolicy;

    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);

    private final ManagedLedgerFactoryImpl mlFactory;
    protected final ManagedLedgerFactoryMBeanImpl mlFactoryMBean;

    protected static final double MB = 1024 * 1024;

    private static final double evictionTriggerThresholdPercent = 0.98;


    public RangeEntryCacheManagerImpl(ManagedLedgerFactoryImpl factory) {
        this.maxSize = factory.getConfig().getMaxCacheSize();
        this.evictionTriggerThreshold = (long) (maxSize * evictionTriggerThresholdPercent);
        this.cacheEvictionWatermark = factory.getConfig().getCacheEvictionWatermark();
        this.evictionPolicy = new EntryCacheDefaultEvictionPolicy();
        this.mlFactory = factory;
        this.mlFactoryMBean = factory.getMbean();

        log.info("Initialized managed-ledger entry cache of {} Mb", maxSize / MB);
    }

    public EntryCache getEntryCache(ManagedLedgerImpl ml) {
        if (maxSize == 0) {
            // Cache is disabled
            return new EntryCacheDisabled(ml);
        }

        EntryCache newEntryCache = new RangeEntryCacheImpl(this, ml, mlFactory.getConfig().isCopyEntriesInCache());
        EntryCache currentEntryCache = caches.putIfAbsent(ml.getName(), newEntryCache);
        if (currentEntryCache != null) {
            return currentEntryCache;
        } else {
            return newEntryCache;
        }
    }

    @Override
    public void updateCacheSizeAndThreshold(long maxSize) {
        this.maxSize = maxSize;
        this.evictionTriggerThreshold = (long) (maxSize * evictionTriggerThresholdPercent);
    }

    @Override
    public void updateCacheEvictionWatermark(double cacheEvictionWatermark) {
        this.cacheEvictionWatermark = cacheEvictionWatermark;
    }

    @Override
    public void removeEntryCache(String name) {
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
            mlFactory.getScheduledExecutor().execute(safeRun(() -> {
                // Trigger a new cache eviction cycle to bring the used memory below the cacheEvictionWatermark
                // percentage limit
                long sizeToEvict = currentSize - (long) (maxSize * cacheEvictionWatermark);
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
        mlFactoryMBean.recordCacheInsertion();
        currentSize.addAndGet(size);
    }

    void entriesRemoved(long size, int count) {
        mlFactoryMBean.recordNumberOfCacheEntriesEvicted(count);
        currentSize.addAndGet(-size);
    }

    @Override
    public long getSize() {
        return currentSize.get();
    }

    @Override
    public long getMaxSize() {
        return maxSize;
    }

    @Override
    public double getCacheEvictionWatermark() {
        return cacheEvictionWatermark;
    }

    @Override
    public void clear() {
        caches.values().forEach(EntryCache::clear);
    }

    public static Entry create(long ledgerId, long entryId, ByteBuf data) {
        return EntryImpl.create(ledgerId, entryId, data);
    }

    public static EntryImpl create(LedgerEntry ledgerEntry, ManagedLedgerInterceptor interceptor) {
        ManagedLedgerInterceptor.PayloadProcessorHandle processorHandle = null;
        if (interceptor != null) {
            ByteBuf duplicateBuffer = ledgerEntry.getEntryBuffer().retainedDuplicate();
            processorHandle = interceptor
                    .processPayloadBeforeEntryCache(duplicateBuffer);
            if (processorHandle != null) {
                ledgerEntry  = LedgerEntryImpl.create(ledgerEntry.getLedgerId(), ledgerEntry.getEntryId(),
                        ledgerEntry.getLength(), processorHandle.getProcessedPayload());
            } else {
                duplicateBuffer.release();
            }
        }
        EntryImpl returnEntry = EntryImpl.create(ledgerEntry);
        if (processorHandle != null) {
            processorHandle.release();
            ledgerEntry.close();
        }
        return returnEntry;
    }

    private static final Logger log = LoggerFactory.getLogger(RangeEntryCacheManagerImpl.class);
}

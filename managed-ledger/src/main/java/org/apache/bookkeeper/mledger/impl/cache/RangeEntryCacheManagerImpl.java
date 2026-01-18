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
package org.apache.bookkeeper.mledger.impl.cache;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:javadoctype")
public class RangeEntryCacheManagerImpl implements EntryCacheManager {

    private volatile long maxSize;
    private volatile long evictionTriggerThreshold;
    private volatile double cacheEvictionWatermark;
    private final AtomicLong currentSize = new AtomicLong(0);
    private final ConcurrentMap<String, EntryCache> caches = new ConcurrentHashMap();
    private final RangeCacheRemovalQueue rangeCacheRemovalQueue;
    private final RangeEntryCacheManagerEvictionHandler evictionHandler;

    private final AtomicReference<CompletableFuture<Void>> evictionInProgress = new AtomicReference<>(null);

    private final ManagedLedgerFactoryImpl mlFactory;
    @Getter
    protected final ManagedLedgerFactoryMBeanImpl mlFactoryMBean;
    private final InflightReadsLimiter inflightReadsLimiter;

    protected static final double MB = 1024 * 1024;
    private static final double evictionTriggerThresholdPercent = 0.98;

    @Setter
    private volatile EntryLengthFunction entryLengthFunction = EntryLengthFunction.DEFAULT;

    public RangeEntryCacheManagerImpl(ManagedLedgerFactoryImpl factory, OrderedScheduler scheduledExecutor,
                                      OpenTelemetry openTelemetry) {
        ManagedLedgerFactoryConfig config = factory.getConfig();
        this.maxSize = config.getMaxCacheSize();
        this.inflightReadsLimiter = new InflightReadsLimiter(config.getManagedLedgerMaxReadsInFlightSize(),
                config.getManagedLedgerMaxReadsInFlightPermitsAcquireQueueSize(),
                config.getManagedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis(),
                scheduledExecutor, openTelemetry);
        this.evictionTriggerThreshold = (long) (maxSize * evictionTriggerThresholdPercent);
        this.cacheEvictionWatermark = config.getCacheEvictionWatermark();
        this.mlFactory = factory;
        this.mlFactoryMBean = factory.getMbean();
        this.rangeCacheRemovalQueue = new RangeCacheRemovalQueue(
                config.getCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(),
                config.isCacheEvictionExtendTTLOfRecentlyAccessed());
        this.evictionHandler = new RangeEntryCacheManagerEvictionHandler(this, rangeCacheRemovalQueue);

        log.info("Initialized managed-ledger entry cache of {} Mb", maxSize / MB);
    }

    public EntryCache getEntryCache(ManagedLedger ml) {
        if (maxSize == 0) {
            // Cache is disabled
            return new EntryCacheDisabled((ManagedLedgerImpl) ml);
        }

        EntryCache newEntryCache =
                new RangeEntryCacheImpl(this, (ManagedLedgerImpl) ml, mlFactory.getConfig().isCopyEntriesInCache(),
                        rangeCacheRemovalQueue, entryLengthFunction);
        EntryCache currentEntryCache = caches.putIfAbsent(ml.getName(), newEntryCache);
        if (currentEntryCache != null) {
            log.warn("Entry cache for {} already exists, newEntryCache: {}, currentEntryCache: {}", ml.getName(),
                    newEntryCache, currentEntryCache);
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

    /**
     * Trigger an eviction cycle if the cache size is over the threshold.
     *
     * @return when eviction is in progress or triggered, return  a future that will be completed when the eviction
     * cycle is completed
     */
    Optional<CompletableFuture<Void>> triggerEvictionWhenNeeded() {
        long currentSize = this.currentSize.get();

        // Trigger a single eviction in background. While the eviction is running we stop inserting entries in the cache
        if (currentSize > evictionTriggerThreshold) {
            CompletableFuture<Void> evictionCompletionFuture = null;
            while (evictionCompletionFuture == null) {
                evictionCompletionFuture = evictionInProgress.get();
                if (evictionCompletionFuture == null) {
                    evictionCompletionFuture = evictionInProgress.updateAndGet(
                            currentValue -> currentValue == null ? new CompletableFuture<>() : null);
                    if (evictionCompletionFuture != null) {
                        triggerEvictionToMakeSpace(evictionCompletionFuture);
                    }
                }
            }
            return Optional.of(evictionCompletionFuture);
        } else {
            return Optional.empty();
        }
    }

    private void triggerEvictionToMakeSpace(CompletableFuture<Void> evictionCompletionFuture) {
        mlFactory.getCacheEvictionExecutor().execute(() -> {
            try {
                // Trigger a new cache eviction cycle to bring the used memory below the cacheEvictionWatermark
                // percentage limit
                doEvictToWatermarkWhenOverThreshold();
            } finally {
                evictionCompletionFuture.complete(null);
                evictionInProgress.set(null);
            }
        });
    }

    private void doEvictToWatermarkWhenOverThreshold() {
        long currentSize = this.currentSize.get();
        if (currentSize > evictionTriggerThreshold) {
            long sizeToEvict = currentSize - (long) (maxSize * cacheEvictionWatermark);
            if (sizeToEvict > 0) {
                try {
                    long startTime = System.nanoTime();
                    if (log.isDebugEnabled()) {
                        log.debug("Triggering cache eviction. total size: {} Mb -- Need to discard: {} Mb",
                                currentSize / MB, sizeToEvict / MB);
                    }
                    evictionHandler.evictEntries(sizeToEvict);
                    if (log.isDebugEnabled()) {
                        long endTime = System.nanoTime();
                        double durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
                        log.debug("Eviction completed. Removed {} Mb in {} ms",
                                (currentSize - this.currentSize.get()) / MB,
                                durationMs);
                    }
                } finally {
                    mlFactoryMBean.recordCacheEviction();
                }
            }
        }
    }

    void entryAdded(long size) {
        currentSize.addAndGet(size);
        mlFactoryMBean.recordCacheInsertion();
        triggerEvictionWhenNeeded();
    }

    void entriesRemoved(long size, int count) {
        mlFactoryMBean.recordNumberOfCacheEntriesEvicted(count);
        currentSize.addAndGet(-size);
    }

    @Override
    public long getSize() {
        return currentSize.get();
    }

    @VisibleForTesting
    public Pair<Integer, Long> getNonEvictableSize() {
        return evictionHandler.getNonEvictableSize();
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
    public void doCacheEviction() {
        // this method is expected to be called from the cache eviction executor
        CompletableFuture<Void> evictionCompletionFuture = new CompletableFuture<>();
        evictionInProgress.set(evictionCompletionFuture);
        try {
            long maxTimestamp = System.nanoTime() - mlFactory.getCacheEvictionTimeThreshold();
            evictionHandler.invalidateEntriesBeforeTimestampNanos(maxTimestamp);
            doEvictToWatermarkWhenOverThreshold();
        } finally {
            evictionCompletionFuture.complete(null);
            evictionInProgress.set(null);
        }
    }

    @Override
    public void clear() {
        caches.values().forEach(EntryCache::clear);
    }

    public InflightReadsLimiter getInflightReadsLimiter() {
        return inflightReadsLimiter;
    }


    @Override
    public void updateCacheEvictionExtendTTLOfEntriesWithRemainingExpectedReadsMaxTimes(
            int extendTTLOfEntriesWithRemainingExpectedReadsMaxTimes) {
        rangeCacheRemovalQueue.setMaxRequeueCountWhenHasExpectedReads(
                extendTTLOfEntriesWithRemainingExpectedReadsMaxTimes);
    }

    @Override
    public void updateCacheEvictionExtendTTLOfRecentlyAccessed(boolean cacheEvictionExtendTTLOfRecentlyAccessed) {
        rangeCacheRemovalQueue.setExtendTTLOfRecentlyAccessed(cacheEvictionExtendTTLOfRecentlyAccessed);
    }

    @VisibleForTesting
    RangeCacheRemovalQueue getRangeCacheRemovalQueue() {
        return rangeCacheRemovalQueue;
    }

    @VisibleForTesting
    void forEachEntry(Consumer<RangeCacheEntryWrapper> consumer) {
        rangeCacheRemovalQueue.forEachEntry(consumer);
    }

    private static final Logger log = LoggerFactory.getLogger(RangeEntryCacheManagerImpl.class);
}

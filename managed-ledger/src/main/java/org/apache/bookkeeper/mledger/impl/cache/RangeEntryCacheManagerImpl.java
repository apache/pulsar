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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.CustomLog;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryMBeanImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyManagedLedgerImpl;
import org.apache.commons.lang3.tuple.Pair;

@CustomLog
@SuppressWarnings("checkstyle:javadoctype")
public class RangeEntryCacheManagerImpl implements EntryCacheManager, AutoCloseable {

    private volatile long maxSize;
    private volatile long evictionTriggerThreshold;
    private volatile double cacheEvictionWatermark;
    private final AtomicLong currentSize = new AtomicLong(0);
    private final ConcurrentMap<String, EntryCache> caches = new ConcurrentHashMap<>();
    private volatile boolean closed;
    // Read-only opens are independent owners, even when another reader or a writer has the same ledger name.
    private final ConcurrentMap<ManagedLedger, EntryCache> readOnlyCaches = new ConcurrentHashMap<>();
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

        log.info().attr("maxSizeMb", maxSize / MB).log("Initialized managed-ledger entry cache");
    }

    public EntryCache getEntryCache(ManagedLedger ml) {
        if (closed || maxSize == 0) {
            // Cache is disabled
            return new EntryCacheDisabled((ManagedLedgerImpl) ml, inflightReadsLimiter);
        }

        EntryCache newEntryCache =
                new RangeEntryCacheImpl(this, (ManagedLedgerImpl) ml, mlFactory.getConfig().isCopyEntriesInCache(),
                        rangeCacheRemovalQueue, entryLengthFunction);
        EntryCache currentEntryCache = ml instanceof ReadOnlyManagedLedgerImpl
                ? readOnlyCaches.putIfAbsent(ml, newEntryCache)
                : caches.putIfAbsent(ml.getName(), newEntryCache);
        EntryCache result = currentEntryCache != null ? currentEntryCache : newEntryCache;
        if (closed) {
            // A constructor racing with factory shutdown must not leave a newly registered cache behind.
            if (ml instanceof ReadOnlyManagedLedgerImpl) {
                removeReadOnlyEntryCache(ml);
            } else {
                removeEntryCache(ml.getName());
            }
        }
        return result;
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
        closeRemovedEntryCache(name, caches.remove(name));
    }

    /** Releases only the cache belonging to this read-only managed-ledger instance. */
    public void removeReadOnlyEntryCache(ManagedLedger owner) {
        closeRemovedEntryCache(owner.getName(), readOnlyCaches.remove(owner));
    }

    private void closeRemovedEntryCache(String name, EntryCache entryCache) {
        if (entryCache == null) {
            return;
        }

        long size = entryCache.getSize();
        if (entryCache instanceof RangeEntryCacheImpl rangeEntryCache) {
            rangeEntryCache.close();
        } else {
            entryCache.clear();
        }

        log.debug().attr("managedLedger", name)
                .attr("sizeMb", size / MB)
                .attr("currentSizeMb", currentSize.get() / MB)
                .log("Removed cache");
    }

    /**
     * Trigger an eviction cycle if the cache size is over the threshold.
     *
     * @return when eviction is in progress or triggered, return  a future that will be completed when the eviction
     * cycle is completed
     */
    Optional<CompletableFuture<Void>> triggerEvictionWhenNeeded() {
        long currentSize = this.currentSize.get();

        // Trigger a single eviction in background. While it is running, a new trigger returns the running eviction's
        // future instead of starting another one. Inserting entries in the cache doesn't wait for it.
        if (currentSize > evictionTriggerThreshold) {
            CompletableFuture<Void> evictionCompletionFuture = null;
            while (evictionCompletionFuture == null) {
                evictionCompletionFuture = evictionInProgress.get();
                if (evictionCompletionFuture == null) {
                    CompletableFuture<Void> newEvictionFuture = new CompletableFuture<>();
                    if (evictionInProgress.compareAndSet(null, newEvictionFuture)) {
                        evictionCompletionFuture = newEvictionFuture;
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
                    log.debug().attr("totalSizeMb", currentSize / MB)
                            .attr("sizeToEvictMb", sizeToEvict / MB)
                            .log("Triggering cache eviction");

                    var event = log.debug().timed();
                    evictionHandler.evictEntries(sizeToEvict);
                    event.attr("removedMb", (currentSize - this.currentSize.get()) / MB)
                            .log("Eviction completed");
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

    /** Whether the cache has reached its eviction watermark, above which reads ahead don't add entries. */
    boolean isAtEvictionWatermark() {
        return maxSize > 0 && getSize() >= (long) (maxSize * cacheEvictionWatermark);
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
        readOnlyCaches.values().forEach(EntryCache::clear);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        caches.forEach((name, cache) -> {
            if (caches.remove(name, cache)) {
                closeRemovedEntryCache(name, cache);
            }
        });
        readOnlyCaches.forEach((owner, cache) -> {
            if (readOnlyCaches.remove(owner, cache)) {
                closeRemovedEntryCache(owner.getName(), cache);
            }
        });
        inflightReadsLimiter.close();
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
    int getReadOnlyEntryCacheCount() {
        return readOnlyCaches.size();
    }

    @VisibleForTesting
    RangeCacheRemovalQueue getRangeCacheRemovalQueue() {
        return rangeCacheRemovalQueue;
    }

    @VisibleForTesting
    void forEachEntry(Consumer<RangeCacheEntryWrapper> consumer) {
        rangeCacheRemovalQueue.forEachEntry(consumer);
    }

}

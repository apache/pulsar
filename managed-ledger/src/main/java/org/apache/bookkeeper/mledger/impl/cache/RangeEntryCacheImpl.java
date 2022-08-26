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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.prometheus.client.Summary;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.util.RangeCache;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache data payload for entries of all ledgers.
 */
public class RangeEntryCacheImpl implements EntryCache {

    static final Summary PULSAR_ML_CACHE_ENTRY_RANGE_SIZE = Summary.build()
            .name("pulsar_ml_cache_entry_range_size")
            .help("Number of entries in a range request")
            .quantile(0.5, 0.1)
            .quantile(0.99, 0.01)
            .register();
    private final RangeEntryCacheManagerImpl manager;
    private final ManagedLedgerImpl ml;
    private ManagedLedgerInterceptor interceptor;
    private final RangeCache<PositionImpl, EntryImpl> entries;
    private final boolean copyEntries;
    private final ConcurrentHashMap<PendingReadKey, CachedPendingRead> cachedPendingReads =
            new ConcurrentHashMap<>();

    private static final double MB = 1024 * 1024;

    @Value
    private static class PendingReadKey {
        private final long ledgerId;
        private final long startEntry;
        private final long endEntry;
    }

    public RangeEntryCacheImpl(RangeEntryCacheManagerImpl manager, ManagedLedgerImpl ml, boolean copyEntries) {
        this.manager = manager;
        this.ml = ml;
        this.interceptor = ml.getManagedLedgerInterceptor();
        this.entries = new RangeCache<>(EntryImpl::getLength, EntryImpl::getTimestamp);
        this.copyEntries = copyEntries;

        if (log.isDebugEnabled()) {
            log.debug("[{}] Initialized managed-ledger entry cache", ml.getName());
        }
    }

    @Override
    public String getName() {
        return ml.getName();
    }

    public static final PooledByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true, // preferDirect
            0, // nHeapArenas,
            PooledByteBufAllocator.defaultNumDirectArena(), // nDirectArena
            PooledByteBufAllocator.defaultPageSize(), // pageSize
            PooledByteBufAllocator.defaultMaxOrder(), // maxOrder
            PooledByteBufAllocator.defaultSmallCacheSize(), // smallCacheSize
            PooledByteBufAllocator.defaultNormalCacheSize(), // normalCacheSize,
            true // Use cache for all threads
    );

    @Override
    public boolean insert(EntryImpl entry) {
        if (!manager.hasSpaceInCache()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Skipping cache while doing eviction: {} - size: {}", ml.getName(), entry.getPosition(),
                        entry.getLength());
            }
            return false;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Adding entry to cache: {} - size: {}", ml.getName(), entry.getPosition(),
                    entry.getLength());
        }

        PositionImpl position = entry.getPosition();
        if (entries.exists(position)) {
            return false;
        }

        ByteBuf cachedData;
        if (copyEntries) {
            cachedData = copyEntry(entry);
            if (cachedData == null) {
                return false;
            }
        } else {
            // Use retain here to have the same counter increase as in the copy entry scenario
            cachedData = entry.getDataBuffer().retain();
        }

        EntryImpl cacheEntry = EntryImpl.create(position, cachedData);
        cachedData.release();
        if (entries.put(position, cacheEntry)) {
            manager.entryAdded(entry.getLength());
            return true;
        } else {
            // entry was not inserted into cache, we need to discard it
            cacheEntry.release();
            return false;
        }
    }

    private ByteBuf copyEntry(EntryImpl entry) {
        // Copy the entry into a buffer owned by the cache. The reason is that the incoming entry is retaining a buffer
        // from netty, usually allocated in 64Kb chunks. So if we just retain the entry without copying it, we might
        // retain actually the full 64Kb even for a small entry
        int size = entry.getLength();
        ByteBuf cachedData = null;
        try {
            cachedData = ALLOCATOR.directBuffer(size, size);
        } catch (Throwable t) {
            log.warn("[{}] Failed to allocate buffer for entry cache: {}", ml.getName(), t.getMessage());
            return null;
        }

        if (size > 0) {
            ByteBuf entryBuf = entry.getDataBuffer();
            int readerIdx = entryBuf.readerIndex();
            cachedData.writeBytes(entryBuf);
            entryBuf.readerIndex(readerIdx);
        }

        return cachedData;
    }

    @Override
    public void invalidateEntries(final PositionImpl lastPosition) {
        final PositionImpl firstPosition = PositionImpl.get(-1, 0);

        if (firstPosition.compareTo(lastPosition) > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Attempted to invalidate entries in an invalid range : {} ~ {}",
                        firstPosition, lastPosition);
            }
            return;
        }

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, false);
        int entriesRemoved = removed.getLeft();
        long sizeRemoved = removed.getRight();
        if (log.isTraceEnabled()) {
            log.trace("[{}] Invalidated entries up to {} - Entries removed: {} - Size removed: {}", ml.getName(),
                    lastPosition, entriesRemoved, sizeRemoved);
        }

        manager.entriesRemoved(sizeRemoved, entriesRemoved);
    }

    @Override
    public void invalidateAllEntries(long ledgerId) {
        final PositionImpl firstPosition = PositionImpl.get(ledgerId, 0);
        final PositionImpl lastPosition = PositionImpl.get(ledgerId + 1, 0);

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, false);
        int entriesRemoved = removed.getLeft();
        long sizeRemoved = removed.getRight();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Invalidated all entries on ledger {} - Entries removed: {} - Size removed: {}",
                    ml.getName(), ledgerId, entriesRemoved, sizeRemoved);
        }

        manager.entriesRemoved(sizeRemoved, entriesRemoved);
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, PositionImpl position, final ReadEntryCallback callback,
            final Object ctx) {
        try {
            asyncReadEntry0(lh, position, callback, ctx);
        } catch (Throwable t) {
            log.warn("failed to read entries for {}-{}", lh.getId(), position, t);
            // invalidate all entries related to ledger from the cache (it might happen if entry gets corrupt
            // (entry.data is already deallocate due to any race-condition) so, invalidate cache and next time read from
            // the bookie)
            invalidateAllEntries(lh.getId());
            callback.readEntryFailed(createManagedLedgerException(t), ctx);
        }
    }

    private void asyncReadEntry0(ReadHandle lh, PositionImpl position, final ReadEntryCallback callback,
            final Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entry ledger {}: {}", ml.getName(), lh.getId(), position.getEntryId());
        }
        EntryImpl entry = entries.get(position);
        if (entry != null) {
            EntryImpl cachedEntry = EntryImpl.create(entry);
            entry.release();
            manager.mlFactoryMBean.recordCacheHit(cachedEntry.getLength());
            callback.readEntryComplete(cachedEntry, ctx);
        } else {
            lh.readAsync(position.getEntryId(), position.getEntryId()).thenAcceptAsync(
                    ledgerEntries -> {
                        try {
                            Iterator<LedgerEntry> iterator = ledgerEntries.iterator();
                            if (iterator.hasNext()) {
                                LedgerEntry ledgerEntry = iterator.next();
                                EntryImpl returnEntry = RangeEntryCacheManagerImpl.create(ledgerEntry, interceptor);

                                manager.mlFactoryMBean.recordCacheMiss(1, returnEntry.getLength());
                                ml.getMbean().addReadEntriesSample(1, returnEntry.getLength());
                                callback.readEntryComplete(returnEntry, ctx);
                            } else {
                                // got an empty sequence
                                callback.readEntryFailed(new ManagedLedgerException("Could not read given position"),
                                                         ctx);
                            }
                        } finally {
                            ledgerEntries.close();
                        }
                    }, ml.getExecutor().chooseThread(ml.getName())).exceptionally(exception -> {
                        ml.invalidateLedgerHandle(lh);
                        callback.readEntryFailed(createManagedLedgerException(exception), ctx);
                        return null;
            });
        }
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, boolean shouldCacheEntry,
            final ReadEntriesCallback callback, Object ctx) {
        try {
            asyncReadEntry0(lh, firstEntry, lastEntry, shouldCacheEntry, callback, ctx);
        } catch (Throwable t) {
            log.warn("failed to read entries for {}--{}-{}", lh.getId(), firstEntry, lastEntry, t);
            // invalidate all entries related to ledger from the cache (it might happen if entry gets corrupt
            // (entry.data is already deallocate due to any race-condition) so, invalidate cache and next time read from
            // the bookie)
            invalidateAllEntries(lh.getId());
            callback.readEntriesFailed(createManagedLedgerException(t), ctx);
        }
    }

    @AllArgsConstructor
    private static final class ReadEntriesCallbackWithContext {
        final ReadEntriesCallback callback;
        final Object ctx;
    }
    private class CachedPendingRead {
        final PendingReadKey key;
        final List<ReadEntriesCallbackWithContext> callbacks = new ArrayList<>(1);
        boolean completed = false;

        public CachedPendingRead(PendingReadKey key) {
            this.key = key;
        }

        public void attach(CompletableFuture<List<EntryImpl>> handle) {
            // when the future is done remove this from the map
            // new reads will go to a new instance
            // this is required because we are going to do refcount management
            // on the results of the callback
            handle.whenComplete((___, error) -> {
                synchronized (CachedPendingRead.this) {
                    completed = true;
                    cachedPendingReads.remove(key, this);
                }
            });

            handle.thenAcceptAsync(entriesToReturn -> {
                synchronized (CachedPendingRead.this) {
                    if (callbacks.size() == 1) {
                        callbacks.get(0)
                                .callback.readEntriesComplete((List) entriesToReturn,
                                        callbacks.get(0).ctx);
                    } else {
                        for (ReadEntriesCallbackWithContext callback : callbacks) {
                            List<EntryImpl> copy = new ArrayList<>(entriesToReturn.size());
                            for (EntryImpl entry : entriesToReturn) {
                                EntryImpl entryCopy = EntryImpl.create(entry);
                                copy.add(entryCopy);
                            }
                            callback.callback.readEntriesComplete((List) copy, callback.ctx);
                        }
                        for (EntryImpl entry : entriesToReturn) {
                            entry.release();
                        }
                    }
                }
            }, ml.getExecutor().chooseThread(ml.getName())).exceptionally(exception -> {
                synchronized (CachedPendingRead.this) {
                    for (ReadEntriesCallbackWithContext callback : callbacks) {
                        if (exception instanceof BKException
                                && ((BKException) exception).getCode() == BKException.Code.TooManyRequestsException) {
                            callback.callback.readEntriesFailed(createManagedLedgerException(exception), callback.ctx);
                        } else {
                            ManagedLedgerException mlException = createManagedLedgerException(exception);
                            callback.callback.readEntriesFailed(mlException, callback.ctx);
                        }
                    }
                }
                return null;
            });
        }

        synchronized boolean addListener(ReadEntriesCallback callback, Object ctx) {
            if (completed) {
                return false;
            }
            callbacks.add(new ReadEntriesCallbackWithContext(callback, ctx));
            return true;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void asyncReadEntry0(ReadHandle lh, long firstEntry, long lastEntry, boolean shouldCacheEntry,
            final ReadEntriesCallback callback, Object ctx) {
        final long ledgerId = lh.getId();
        final int entriesToRead = (int) (lastEntry - firstEntry) + 1;
        final PositionImpl firstPosition = PositionImpl.get(lh.getId(), firstEntry);
        final PositionImpl lastPosition = PositionImpl.get(lh.getId(), lastEntry);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entries range ledger {}: {} to {}", ml.getName(), ledgerId, firstEntry, lastEntry);
        }

        PULSAR_ML_CACHE_ENTRY_RANGE_SIZE.observe(entriesToRead);
        Collection<EntryImpl> cachedEntries = entries.getRange(firstPosition, lastPosition);

        if (cachedEntries.size() == entriesToRead) {
            long totalCachedSize = 0;
            final List<EntryImpl> entriesToReturn = Lists.newArrayListWithExpectedSize(entriesToRead);

            // All entries found in cache
            for (EntryImpl entry : cachedEntries) {
                entriesToReturn.add(EntryImpl.create(entry));
                totalCachedSize += entry.getLength();
                entry.release();
            }

            manager.mlFactoryMBean.recordCacheHits(entriesToReturn.size(), totalCachedSize);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ledger {} -- Found in cache entries: {}-{}", ml.getName(), ledgerId, firstEntry,
                        lastEntry);
            }

            callback.readEntriesComplete((List) entriesToReturn, ctx);

        } else {
            if (!cachedEntries.isEmpty()) {
                cachedEntries.forEach(entry -> entry.release());
            }

            // Read all the entries from bookkeeper
            final PendingReadKey key = new PendingReadKey(lh.getId(), firstEntry, lastEntry);

            boolean listenerAdded = false;
            while (!listenerAdded) {
                AtomicBoolean createdByThisThread = new AtomicBoolean();
                CachedPendingRead cachedPendingRead = cachedPendingReads.computeIfAbsent(key, operationKey -> {
                    createdByThisThread.set(true);
                    return new CachedPendingRead(operationKey);
                });
                listenerAdded = cachedPendingRead.addListener(callback, ctx);

                if (createdByThisThread.get()) {
                    CompletableFuture<List<EntryImpl>> readResult = lh.readAsync(firstEntry, lastEntry)
                            .thenApply(
                                    ledgerEntries -> {
                                        requireNonNull(ml.getName());
                                        requireNonNull(ml.getExecutor());

                                        try {
                                            // We got the entries, we need to transform them to a List<> type
                                            long totalSize = 0;
                                            final List<EntryImpl> entriesToReturn =
                                                    Lists.newArrayListWithExpectedSize(entriesToRead);
                                            for (LedgerEntry e : ledgerEntries) {
                                                EntryImpl entry = RangeEntryCacheManagerImpl.create(e, interceptor);
                                                entriesToReturn.add(entry);
                                                totalSize += entry.getLength();
                                                if (shouldCacheEntry) {
                                                    EntryImpl cacheEntry = EntryImpl.create(entry);
                                                    insert(cacheEntry);
                                                    cacheEntry.release();
                                                }
                                            }

                                            manager.mlFactoryMBean.recordCacheMiss(entriesToReturn.size(), totalSize);
                                            ml.getMbean().addReadEntriesSample(entriesToReturn.size(), totalSize);

                                            return entriesToReturn;
                                        } finally {
                                            ledgerEntries.close();
                                        }
                                    });
                    // handle LH invalidation
                    readResult.exceptionally(exception -> {
                        if (exception instanceof BKException
                                && ((BKException) exception).getCode() == BKException.Code.TooManyRequestsException) {
                        } else {
                            ml.invalidateLedgerHandle(lh);
                        }
                        return null;
                    });

                    cachedPendingRead.attach(readResult);
                }
            }
        }
    }

    @Override
    public void clear() {
        Pair<Integer, Long> removedPair = entries.clear();
        manager.entriesRemoved(removedPair.getRight(), removedPair.getLeft());
        cachedPendingReads.clear();
    }

    @Override
    public long getSize() {
        return entries.getSize();
    }

    @Override
    public int compareTo(EntryCache other) {
        return Longs.compare(getSize(), other.getSize());
    }

    @Override
    public Pair<Integer, Long> evictEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        Pair<Integer, Long> evicted = entries.evictLeastAccessedEntries(sizeToFree);
        int evictedEntries = evicted.getLeft();
        long evictedSize = evicted.getRight();
        if (log.isDebugEnabled()) {
            log.debug(
                    "[{}] Doing cache eviction of at least {} Mb -- Deleted {} entries - Total size deleted: {} Mb "
                            + " -- Current Size: {} Mb",
                    ml.getName(), sizeToFree / MB, evictedEntries, evictedSize / MB, entries.getSize() / MB);
        }
        manager.entriesRemoved(evictedSize, evictedEntries);
        return evicted;
    }

    @Override
    public void invalidateEntriesBeforeTimestamp(long timestamp) {
        Pair<Integer, Long> evictedPair = entries.evictLEntriesBeforeTimestamp(timestamp);
        manager.entriesRemoved(evictedPair.getRight(), evictedPair.getLeft());
    }

    private static final Logger log = LoggerFactory.getLogger(RangeEntryCacheImpl.class);
}

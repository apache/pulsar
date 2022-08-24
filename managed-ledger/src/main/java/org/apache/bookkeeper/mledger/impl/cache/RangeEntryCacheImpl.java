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
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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

    private final RangeEntryCacheManagerImpl manager;
    private final ManagedLedgerImpl ml;
    private ManagedLedgerInterceptor interceptor;
    private final RangeCache<PositionImpl, EntryImpl> entries;
    private final boolean copyEntries;

    private static final double MB = 1024 * 1024;

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

        PositionImpl position = entry.getPosition();
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
    public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
            final ReadEntriesCallback callback, Object ctx) {
        try {
            asyncReadEntry0(lh, firstEntry, lastEntry, isSlowestReader, callback, ctx);
        } catch (Throwable t) {
            log.warn("failed to read entries for {}--{}-{}", lh.getId(), firstEntry, lastEntry, t);
            // invalidate all entries related to ledger from the cache (it might happen if entry gets corrupt
            // (entry.data is already deallocate due to any race-condition) so, invalidate cache and next time read from
            // the bookie)
            invalidateAllEntries(lh.getId());
            callback.readEntriesFailed(createManagedLedgerException(t), ctx);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void asyncReadEntry0(ReadHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
            final ReadEntriesCallback callback, Object ctx) {
        final long ledgerId = lh.getId();
        final int entriesToRead = (int) (lastEntry - firstEntry) + 1;
        final PositionImpl firstPosition = PositionImpl.get(lh.getId(), firstEntry);
        final PositionImpl lastPosition = PositionImpl.get(lh.getId(), lastEntry);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entries range ledger {}: {} to {}", ml.getName(), ledgerId, firstEntry, lastEntry);
        }

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
            lh.readAsync(firstEntry, lastEntry).thenAcceptAsync(
                    ledgerEntries -> {
                        checkNotNull(ml.getName());
                        checkNotNull(ml.getExecutor());

                        try {
                            // We got the entries, we need to transform them to a List<> type
                            long totalSize = 0;
                            final List<EntryImpl> entriesToReturn = Lists.newArrayListWithExpectedSize(entriesToRead);
                            for (LedgerEntry e : ledgerEntries) {
                                EntryImpl entry = RangeEntryCacheManagerImpl.create(e, interceptor);

                                entriesToReturn.add(entry);
                                totalSize += entry.getLength();
                            }

                            manager.mlFactoryMBean.recordCacheMiss(entriesToReturn.size(), totalSize);
                            ml.getMbean().addReadEntriesSample(entriesToReturn.size(), totalSize);

                            callback.readEntriesComplete((List) entriesToReturn, ctx);
                        } finally {
                            ledgerEntries.close();
                        }
                    }, ml.getExecutor().chooseThread(ml.getName())).exceptionally(exception -> {
                        if (exception instanceof BKException
                                && ((BKException) exception).getCode() == BKException.Code.TooManyRequestsException) {
                            callback.readEntriesFailed(createManagedLedgerException(exception), ctx);
                        } else {
                            ml.invalidateLedgerHandle(lh);
                            ManagedLedgerException mlException = createManagedLedgerException(exception);
                            callback.readEntriesFailed(mlException, ctx);
                        }
                        return null;
            });
        }
    }

    @Override
    public void clear() {
        Pair<Integer, Long> removedPair = entries.clear();
        manager.entriesRemoved(removedPair.getRight(), removedPair.getLeft());
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

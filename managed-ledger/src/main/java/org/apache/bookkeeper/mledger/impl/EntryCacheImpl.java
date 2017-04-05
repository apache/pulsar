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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import java.util.Collection;
import java.util.List;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.bookkeeper.mledger.util.RangeCache;
import org.apache.bookkeeper.mledger.util.RangeCache.Weighter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Cache data payload for entries of all ledgers
 */
public class EntryCacheImpl implements EntryCache {

    private final EntryCacheManager manager;
    private final ManagedLedgerImpl ml;
    private final RangeCache<PositionImpl, EntryImpl> entries;

    private static final double MB = 1024 * 1024;

    private static final Weighter<EntryImpl> entryWeighter = new Weighter<EntryImpl>() {
        public long getSize(EntryImpl entry) {
            return entry.getLength();
        }
    };

    public EntryCacheImpl(EntryCacheManager manager, ManagedLedgerImpl ml) {
        this.manager = manager;
        this.ml = ml;
        this.entries = new RangeCache<PositionImpl, EntryImpl>(entryWeighter);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Initialized managed-ledger entry cache", ml.getName());
        }
    }

    @Override
    public String getName() {
        return ml.getName();
    }

    public final static PooledByteBufAllocator allocator = new PooledByteBufAllocator( //
            true, // preferDirect
            0, // nHeapArenas,
            1, // nDirectArena
            8192, // pageSize
            11, // maxOrder
            64, // tinyCacheSize
            32, // smallCacheSize
            8 // normalCacheSize
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

        // Copy the entry into a buffer owned by the cache. The reason is that the incoming entry is retaining a buffer
        // from netty, usually allocated in 64Kb chunks. So if we just retain the entry without copying it, we might
        // retain actually the full 64Kb even for a small entry
        int size = entry.getLength();
        ByteBuf cachedData = null;
        try {
            cachedData = allocator.directBuffer(size, size);
        } catch (Throwable t) {
            log.warn("[{}] Failed to allocate buffer for entry cache: {}", ml.getName(), t.getMessage(), t);
            return false;
        }

        if (size > 0) {
            ByteBuf entryBuf = entry.getDataBuffer();
            int readerIdx = entryBuf.readerIndex();
            cachedData.writeBytes(entryBuf);
            entryBuf.readerIndex(readerIdx);
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

    @Override
    public void invalidateEntries(final PositionImpl lastPosition) {
        final PositionImpl firstPosition = PositionImpl.get(-1, 0);

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, true);
        int entriesRemoved = removed.first;
        long sizeRemoved = removed.second;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Invalidated entries up to {} - Entries removed: {} - Size removed: {}", ml.getName(),
                    lastPosition, entriesRemoved, sizeRemoved);
        }

        manager.entriesRemoved(sizeRemoved);
    }

    @Override
    public void invalidateAllEntries(long ledgerId) {
        final PositionImpl firstPosition = PositionImpl.get(ledgerId, 0);
        final PositionImpl lastPosition = PositionImpl.get(ledgerId + 1, 0);

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, false);
        int entriesRemoved = removed.first;
        long sizeRemoved = removed.second;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Invalidated all entries on ledger {} - Entries removed: {} - Size removed: {}",
                    ml.getName(), ledgerId, entriesRemoved, sizeRemoved);
        }

        manager.entriesRemoved(sizeRemoved);
    }

    @Override
    public void asyncReadEntry(LedgerHandle lh, PositionImpl position, final ReadEntryCallback callback,
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
            ReadCallback readCallback = (rc, ledgerHandle, sequence, obj) -> {
                if (rc != BKException.Code.OK) {
                    ml.invalidateLedgerHandle(ledgerHandle, rc);
                    callback.readEntryFailed(new ManagedLedgerException(BKException.create(rc)), obj);
                    return;
                }

                if (sequence.hasMoreElements()) {
                    LedgerEntry ledgerEntry = sequence.nextElement();
                    EntryImpl returnEntry = EntryImpl.create(ledgerEntry);

                    // The EntryImpl is now the owner of the buffer, so we can release the original one
                    ledgerEntry.getEntryBuffer().release();

                    manager.mlFactoryMBean.recordCacheMiss(1, returnEntry.getLength());
                    ml.mbean.addReadEntriesSample(1, returnEntry.getLength());

                    callback.readEntryComplete(returnEntry, obj);
                } else {
                    // got an empty sequence
                    callback.readEntryFailed(new ManagedLedgerException("Could not read given position"), obj);
                }
            };
            lh.asyncReadEntries(position.getEntryId(), position.getEntryId(), readCallback, ctx);
        }
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void asyncReadEntry(LedgerHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
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
            lh.asyncReadEntries(firstEntry, lastEntry, (rc, lh1, sequence, cb) -> {

                if (rc != BKException.Code.OK) {
                    if (rc == BKException.Code.TooManyRequestsException) {
                        callback.readEntriesFailed(new TooManyRequestsException("Too many request error from bookies"),
                                ctx);
                    } else {
                        ml.invalidateLedgerHandle(lh1, rc);
                        callback.readEntriesFailed(new ManagedLedgerException(BKException.getMessage(rc)), ctx);
                    }
                    return;
                }

                checkNotNull(ml.getName());
                checkNotNull(ml.getExecutor());
                ml.getExecutor().submitOrdered(ml.getName(), safeRun(() -> {
                    // We got the entries, we need to transform them to a List<> type
                    long totalSize = 0;
                    final List<EntryImpl> entriesToReturn = Lists.newArrayListWithExpectedSize(entriesToRead);
                    while (sequence.hasMoreElements()) {
                        // Insert the entries at the end of the list (they will be unsorted for now)
                        LedgerEntry ledgerEntry = sequence.nextElement();
                        EntryImpl entry = EntryImpl.create(ledgerEntry);
                        ledgerEntry.getEntryBuffer().release();

                        entriesToReturn.add(entry);

                        totalSize += entry.getLength();

                    }

                    manager.mlFactoryMBean.recordCacheMiss(entriesToReturn.size(), totalSize);
                    ml.getMBean().addReadEntriesSample(entriesToReturn.size(), totalSize);

                    callback.readEntriesComplete((List) entriesToReturn, ctx);
                }));
            }, callback);
        }
    }

    @Override
    public void clear() {
        long removedSize = entries.clear();
        manager.entriesRemoved(removedSize);
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
        int evictedEntries = evicted.first;
        long evictedSize = evicted.second;
        if (log.isDebugEnabled()) {
            log.debug(
                    "[{}] Doing cache eviction of at least {} Mb -- Deleted {} entries - Total size deleted: {} Mb "
                            + " -- Current Size: {} Mb",
                    ml.getName(), sizeToFree / MB, evictedEntries, evictedSize / MB, entries.getSize() / MB);
        }
        manager.entriesRemoved(evictedSize);
        return evicted;
    }

    private static final Logger log = LoggerFactory.getLogger(EntryCacheImpl.class);
}

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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache data payload for entries of all ledgers.
 */
public class RangeEntryCacheImpl implements EntryCache {
    /**
     * The Netty allocator used when managedLedgerCacheCopyEntries=true.
     */
    public static final PooledByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true, // preferDirect
            0, // nHeapArenas,
            PooledByteBufAllocator.defaultNumDirectArena(), // nDirectArena
            PooledByteBufAllocator.defaultPageSize(), // pageSize
            PooledByteBufAllocator.defaultMaxOrder(), // maxOrder
            PooledByteBufAllocator.defaultSmallCacheSize(), // smallCacheSize
            PooledByteBufAllocator.defaultNormalCacheSize(), // normalCacheSize,
            true // Use cache for all threads
    );

    /**
     * Overhead per-entry to take into account the envelope.
     */
    public static final long BOOKKEEPER_READ_OVERHEAD_PER_ENTRY = 64;
    public static final int DEFAULT_ESTIMATED_ENTRY_SIZE = 10 * 1024;
    private static final boolean DEFAULT_CACHE_INDIVIDUAL_READ_ENTRY = false;

    private final RangeEntryCacheManagerImpl manager;
    final ManagedLedgerImpl ml;
    private ManagedLedgerInterceptor interceptor;
    private final RangeCache entries;
    private final boolean copyEntries;
    private final PendingReadsManager pendingReadsManager;

    private static final double MB = 1024 * 1024;

    private final LongAdder totalAddedEntriesSize = new LongAdder();
    private final LongAdder totalAddedEntriesCount = new LongAdder();
    private final EntryLengthFunction entryLengthFunction;

    public RangeEntryCacheImpl(RangeEntryCacheManagerImpl manager, ManagedLedgerImpl ml, boolean copyEntries,
                               RangeCacheRemovalQueue rangeCacheRemovalQueue, EntryLengthFunction entryLengthFunction) {
        this(manager, ml, copyEntries, rangeCacheRemovalQueue, entryLengthFunction, null);
    }

    RangeEntryCacheImpl(RangeEntryCacheManagerImpl manager, ManagedLedgerImpl ml, boolean copyEntries,
                        RangeCacheRemovalQueue rangeCacheRemovalQueue, EntryLengthFunction entryLengthFunction,
                        PendingReadsManager pendingReadsManager) {
        this.manager = manager;
        this.ml = ml;
        this.pendingReadsManager = pendingReadsManager == null ? new PendingReadsManager(this) : pendingReadsManager;
        this.entryLengthFunction = entryLengthFunction;
        this.interceptor = ml.getManagedLedgerInterceptor();
        this.entries = new RangeCache(rangeCacheRemovalQueue);
        this.copyEntries = copyEntries;

        if (log.isDebugEnabled()) {
            log.debug("[{}] Initialized managed-ledger entry cache", ml.getName());
        }
    }

    @VisibleForTesting
    ManagedLedgerImpl getManagedLedger() {
        return ml;
    }

    @VisibleForTesting
    ManagedLedgerConfig getManagedLedgerConfig() {
        return ml.getConfig();
    }

    @Override
    public String getName() {
        return ml.getName();
    }

    @VisibleForTesting
    public InflightReadsLimiter getPendingReadsLimiter() {
        return manager.getInflightReadsLimiter();
    }

    @Override
    public boolean insert(Entry entry) {
        int entryLength = entryLengthFunction.getEntryLength(ml, entry);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Adding entry to cache: {} - size: {}", ml.getName(), entry.getPosition(),
                    entryLength);
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

        Position position = entry.getPosition();
        ReferenceCountedEntry cacheEntry =
                EntryImpl.createWithRetainedDuplicate(position, cachedData, entry.getReadCountHandler());
        cachedData.release();
        if (entries.put(position, cacheEntry, entryLength)) {
            totalAddedEntriesSize.add(entryLength);
            totalAddedEntriesCount.increment();
            manager.entryAdded(entryLength);
            return true;
        } else {
            // entry was not inserted into cache, we need to discard it
            cacheEntry.release();
            return false;
        }
    }

    private ByteBuf copyEntry(Entry entry) {
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
    public void invalidateEntries(final Position lastPosition) {
        final Position firstPosition = PositionFactory.create(-1, 0);

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
        final Position firstPosition = PositionFactory.create(ledgerId, 0);
        final Position lastPosition = PositionFactory.create(ledgerId + 1, 0);

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, false);
        int entriesRemoved = removed.getLeft();
        long sizeRemoved = removed.getRight();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Invalidated all entries on ledger {} - Entries removed: {} - Size removed: {}",
                    ml.getName(), ledgerId, entriesRemoved, sizeRemoved);
        }

        manager.entriesRemoved(sizeRemoved, entriesRemoved);
        pendingReadsManager.invalidateLedger(ledgerId);
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, Position position, final ReadEntryCallback callback,
            final Object ctx) {
        try {
            asyncReadEntriesByPosition(lh, position, position, 1,
                    () -> DEFAULT_CACHE_INDIVIDUAL_READ_ENTRY ? 1 : 0,
                    new ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    if (entries.isEmpty()) {
                        callback.readEntryFailed(new ManagedLedgerException("Could not read given position"), ctx);
                    } else {
                        callback.readEntryComplete(entries.get(0), ctx);
                    }
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    callback.readEntryFailed(exception, ctx);
                }
            }, ctx, true);
        } catch (Throwable t) {
            log.warn("failed to read entries for {}-{}", lh.getId(), position, t);
            // invalidate all entries related to ledger from the cache (it might happen if entry gets corrupt
            // (entry.data is already deallocate due to any race-condition) so, invalidate cache and next time read from
            // the bookie)
            invalidateAllEntries(lh.getId());
            callback.readEntryFailed(createManagedLedgerException(t), ctx);
        }
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, IntSupplier expectedReadCount,
            final ReadEntriesCallback callback, Object ctx) {
        try {
            asyncReadEntry0(lh, firstEntry, lastEntry, expectedReadCount, callback, ctx, true);
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
    void asyncReadEntry0(ReadHandle lh, long firstEntry, long lastEntry, IntSupplier expectedReadCount,
            final ReadEntriesCallback callback, Object ctx, boolean acquirePermits) {
        final long ledgerId = lh.getId();
        final int numberOfEntries = (int) (lastEntry - firstEntry) + 1;
        final Position firstPosition = PositionFactory.create(ledgerId, firstEntry);
        final Position lastPosition = PositionFactory.create(ledgerId, lastEntry);
        asyncReadEntriesByPosition(lh, firstPosition, lastPosition, numberOfEntries, expectedReadCount, callback, ctx,
                acquirePermits);
    }

    void asyncReadEntriesByPosition(ReadHandle lh, Position firstPosition, Position lastPosition, int numberOfEntries,
                                    IntSupplier expectedReadCount, final ReadEntriesCallback originalCallback,
                                    Object ctx, boolean acquirePermits) {
        checkArgument(firstPosition.getLedgerId() == lastPosition.getLedgerId(),
                "Invalid range. Entries %s and %s should be in the same ledger.",
                firstPosition, lastPosition);
        checkArgument(firstPosition.getLedgerId() == lh.getId(),
                "Invalid ReadHandle. The ledger %s of the range positions should match the handle's ledger %s.",
                firstPosition.getLedgerId(), lh.getId());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading {} entries in range {} to {}", ml.getName(), numberOfEntries, firstPosition,
                    lastPosition);
        }

        InflightReadsLimiter pendingReadsLimiter = getPendingReadsLimiter();
        if (!acquirePermits || pendingReadsLimiter.isDisabled()) {
            doAsyncReadEntriesByPosition(lh, firstPosition, lastPosition, numberOfEntries, expectedReadCount,
                    originalCallback, ctx);
        } else {
            long estimatedEntrySize = getEstimatedEntrySize(lh);
            long estimatedReadSize = numberOfEntries * estimatedEntrySize;
            if (log.isDebugEnabled()) {
                log.debug("Estimated read size: {} bytes for {} entries with {} estimated entry size",
                        estimatedReadSize,
                        numberOfEntries, estimatedEntrySize);
            }
            Optional<InflightReadsLimiter.Handle> optionalHandle =
                    pendingReadsLimiter.acquire(estimatedReadSize, handle -> {
                        // permits were not immediately available, callback will be executed when permits are acquired
                        // or timeout
                        ml.getExecutor().execute(() -> {
                            doAsyncReadEntriesWithAcquiredPermits(lh, firstPosition, lastPosition, numberOfEntries,
                                    expectedReadCount, originalCallback, ctx, handle, estimatedReadSize);
                        });
                    });
            // permits were immediately available and acquired
            if (optionalHandle.isPresent()) {
                doAsyncReadEntriesWithAcquiredPermits(lh, firstPosition, lastPosition, numberOfEntries,
                        expectedReadCount, originalCallback, ctx, optionalHandle.get(), estimatedReadSize);
            }
        }
    }

    void doAsyncReadEntriesWithAcquiredPermits(ReadHandle lh, Position firstPosition, Position lastPosition,
                                               int numberOfEntries, IntSupplier expectedReadCount,
                                               final ReadEntriesCallback originalCallback, Object ctx,
                                               InflightReadsLimiter.Handle handle, long estimatedReadSize) {
        if (!handle.success()) {
            String message = String.format(
                    "Couldn't acquire enough permits on the max reads in flight limiter to read from ledger "
                            + "%d, %s, estimated read size %d bytes for %d entries (check "
                            + "managedLedgerMaxReadsInFlightPermitsAcquireQueueSize (direct config), "
                            + "managedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis and "
                            + "managedLedgerMaxReadsInFlightSizeInMB)", lh.getId(), getName(),
                    estimatedReadSize, numberOfEntries);
            originalCallback.readEntriesFailed(new ManagedLedgerException.TooManyRequestsException(message), ctx);
            return;
        }
        InflightReadsLimiter pendingReadsLimiter = getPendingReadsLimiter();
        ReadEntriesCallback wrappedCallback = new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx2) {
                if (!entries.isEmpty()) {
                    // release permits only when entries have been handled
                    AtomicInteger remainingCount = new AtomicInteger(entries.size());
                    for (Entry entry : entries) {
                        ((EntryImpl) entry).onDeallocate(() -> {
                            if (remainingCount.decrementAndGet() <= 0) {
                                pendingReadsLimiter.release(handle);
                            }
                        });
                    }
                } else {
                    pendingReadsLimiter.release(handle);
                }
                originalCallback.readEntriesComplete(entries, ctx2);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx2) {
                pendingReadsLimiter.release(handle);
                originalCallback.readEntriesFailed(exception, ctx2);
            }
        };
        doAsyncReadEntriesByPosition(lh, firstPosition, lastPosition, numberOfEntries, expectedReadCount,
                wrappedCallback, ctx);
    }

    void doAsyncReadEntriesByPosition(ReadHandle lh, Position firstPosition, Position lastPosition, int numberOfEntries,
                                      IntSupplier expectedReadCount, final ReadEntriesCallback callback,
                                      Object ctx) {
        Collection<ReferenceCountedEntry> cachedEntries;
        if (firstPosition.compareTo(lastPosition) == 0) {
            ReferenceCountedEntry cachedEntry = entries.get(firstPosition);
            if (cachedEntry == null) {
                cachedEntries = Collections.emptyList();
            } else {
                cachedEntries = Collections.singleton(cachedEntry);
            }
        } else {
            cachedEntries = entries.getRange(firstPosition, lastPosition);
        }

        if (cachedEntries.size() > 0) {
            long totalCachedSize = 0;
            final List<Entry> entriesToReturn = new ArrayList<>(numberOfEntries);
            for (int i = 0; i < numberOfEntries; i++) {
                entriesToReturn.add(null); // Initialize with nulls
            }

            for (Entry entry : cachedEntries) {
                int index = (int) (entry.getPosition().getEntryId() - firstPosition.getEntryId());
                entriesToReturn.set(index, EntryImpl.create(entry));
                totalCachedSize += entry.getLength();
                entry.release();
            }

            manager.getMlFactoryMBean().recordCacheHits(cachedEntries.size(), totalCachedSize);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cache hit for {} entries in range {} to {}", ml.getName(), numberOfEntries,
                        firstPosition, lastPosition);
            }

            if (cachedEntries.size() == numberOfEntries) {
                callback.readEntriesComplete(entriesToReturn, ctx);
            } else {
                // read missing ranges
                long firstEntryInRange = -1;
                List<CompletableFuture<List<Entry>>> futures = new ArrayList<>();
                for (int i = 0; i < numberOfEntries; i++) {
                    if (entriesToReturn.get(i) == null) {
                        if (firstEntryInRange == -1) {
                            firstEntryInRange = firstPosition.getEntryId() + i;
                        }
                    } else {
                        if (firstEntryInRange != -1) {
                            futures.add(
                                    readMissingEntriesAsync(lh, firstEntryInRange, firstPosition.getEntryId() + i - 1,
                                            expectedReadCount, ctx));
                            firstEntryInRange = -1;
                        }
                    }
                }
                if (firstEntryInRange != -1) {
                    futures.add(
                            readMissingEntriesAsync(lh, firstEntryInRange, lastPosition.getEntryId(),
                                    expectedReadCount, ctx));
                }
                FutureUtil.waitForAll(futures).whenComplete((__, t) -> {
                    if (t != null) {
                        // release cached entries placed in entriesToReturn
                        for (Entry entry : entriesToReturn) {
                            if (entry != null) {
                                entry.release();
                            }
                        }
                        // release entries for futures which were completed successfully
                        for (CompletableFuture<List<Entry>> future : futures) {
                            if (!future.isCompletedExceptionally()) {
                                List<Entry> readEntries = future.getNow(null);
                                if (readEntries != null && !readEntries.isEmpty()) {
                                    for (Entry entry : readEntries) {
                                        entry.release();
                                    }
                                }
                            }
                        }
                        log.warn("Failed to read missing entries from bookkeeper, retrying by reading all", t);
                        // Read all the entries from bookkeeper
                        pendingReadsManager.readEntries(lh, firstPosition.getEntryId(), lastPosition.getEntryId(),
                                expectedReadCount, callback, ctx);
                        return;
                    }
                    for (CompletableFuture<List<Entry>> future : futures) {
                        List<Entry> readEntries = future.getNow(null);
                        if (readEntries != null && !readEntries.isEmpty()) {
                            for (Entry entry : readEntries) {
                                int index = (int) (entry.getPosition().getEntryId() - firstPosition.getEntryId());
                                if (index >= 0 && index < entriesToReturn.size()) {
                                    entriesToReturn.set(index, entry);
                                } else {
                                    log.warn("Received entry {} outside of expected range {} to {}",
                                            entry.getPosition(), firstPosition, lastPosition);
                                }
                            }
                        }
                    }
                    callback.readEntriesComplete(entriesToReturn, ctx);
                });
            }
        } else {
            // Read all the entries from bookkeeper
            pendingReadsManager.readEntries(lh, firstPosition.getEntryId(), lastPosition.getEntryId(),
                    expectedReadCount, callback, ctx);
        }
    }

    private CompletableFuture<List<Entry>> readMissingEntriesAsync(ReadHandle lh,
                                                                   long firstEntry, long lastEntry,
                                                                   IntSupplier expectedReadCount, Object ctx) {
        CompletableFuture<List<Entry>> future = new CompletableFuture<>();
        PendingReadsManager.ReadEntriesCallback callback = new PendingReadsManager.ReadEntriesCallback(future);
        pendingReadsManager.readEntries(lh, firstEntry, lastEntry, expectedReadCount, callback, ctx);
        return future;
    }

    @VisibleForTesting
    public long getEstimatedEntrySize(ReadHandle lh) {
        if (lh.getLength() == 0 || lh.getLastAddConfirmed() < 0) {
            // No entries stored.
            return Math.max(getAvgEntrySize(), DEFAULT_ESTIMATED_ENTRY_SIZE) + BOOKKEEPER_READ_OVERHEAD_PER_ENTRY;
        }
        return Math.max(1, lh.getLength() / (lh.getLastAddConfirmed() + 1)) + BOOKKEEPER_READ_OVERHEAD_PER_ENTRY;
    }

    private long getAvgEntrySize() {
        long totalAddedEntriesCount = this.totalAddedEntriesCount.sum();
        long totalAddedEntriesSize = this.totalAddedEntriesSize.sum();
        return totalAddedEntriesCount != 0 ? totalAddedEntriesSize / totalAddedEntriesCount : 0;
    }

    /**
     * Reads the entries from Storage.
     * @param lh the handle
     * @param firstEntry the first entry
     * @param lastEntry the last entry
     * @param expectedReadCount if we should put the entry into the cache
     * @return a handle to the operation
     */
    CompletableFuture<List<Entry>> readFromStorage(ReadHandle lh, long firstEntry, long lastEntry,
                                                   IntSupplier expectedReadCount) {
        final int entriesToRead = (int) (lastEntry - firstEntry) + 1;
        CompletableFuture<List<Entry>> readResult = ReadEntryUtils.readAsync(ml, lh, firstEntry, lastEntry)
                .thenApply(
                        ledgerEntries -> {
                            requireNonNull(ml.getName());
                            requireNonNull(ml.getExecutor());

                            try {
                                // We got the entries, we need to transform them to a List<> type
                                long totalSize = 0;
                                int expectedReadCountVal = expectedReadCount.getAsInt();
                                final List<Entry> entriesToReturn = new ArrayList<>(entriesToRead);
                                for (LedgerEntry e : ledgerEntries) {
                                    EntryImpl entry = EntryImpl.create(e, interceptor, expectedReadCountVal);
                                    entriesToReturn.add(entry);
                                    totalSize += entry.getLength();
                                    if (expectedReadCountVal > 0) {
                                        insert(entry);
                                    }
                                }

                                ml.getMbean().recordReadEntriesOpsCacheMisses(entriesToReturn.size(), totalSize);
                                manager.getMlFactoryMBean().recordCacheMiss(entriesToReturn.size(), totalSize);
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
                pendingReadsManager.invalidateLedger(lh.getId());
            }
            return null;
        });
        return readResult;
    }

    @Override
    public void clear() {
        Pair<Integer, Long> removedPair = entries.clear();
        manager.entriesRemoved(removedPair.getRight(), removedPair.getLeft());
        pendingReadsManager.clear();
    }

    @Override
    public long getSize() {
        return entries.getSize();
    }

    private static final Logger log = LoggerFactory.getLogger(RangeEntryCacheImpl.class);
}

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

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;

/**
 * Implementation of cache that always read from BookKeeper.
 */
public class EntryCacheDisabled implements EntryCache {
    private final ManagedLedgerImpl ml;
    private final ManagedLedgerInterceptor interceptor;
    private final InflightReadsLimiter inflightReadsLimiter;

    public EntryCacheDisabled(ManagedLedgerImpl ml) {
        this(ml, null);
    }

    EntryCacheDisabled(ManagedLedgerImpl ml, InflightReadsLimiter inflightReadsLimiter) {
        this.ml = ml;
        this.interceptor = ml.getManagedLedgerInterceptor();
        this.inflightReadsLimiter = inflightReadsLimiter;
    }

    @Override
    public String getName() {
        return ml.getName();
    }

    @Override
    public boolean insert(Entry entry) {
        return false;
    }

    @Override
    public void invalidateEntries(Position lastPosition) {
    }

    @Override
    public void invalidateAllEntries(long ledgerId) {
    }

    @Override
    public void clear() {
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, IntSupplier expectedReadCount,
                               final AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        if (inflightReadsLimiter == null || inflightReadsLimiter.isDisabled()) {
            readEntries(lh, firstEntry, lastEntry, callback, ctx);
            return;
        }

        long estimatedReadSize = (lastEntry - firstEntry + 1) * getEstimatedEntrySize(lh);
        Optional<InflightReadsLimiter.Handle> optionalHandle = inflightReadsLimiter.acquire(estimatedReadSize, handle ->
                ml.getExecutor().execute(() -> readEntriesWithAcquiredPermits(lh, firstEntry, lastEntry, callback,
                        ctx, handle)));
        optionalHandle.ifPresent(handle -> readEntriesWithAcquiredPermits(lh, firstEntry, lastEntry, callback, ctx,
                handle));
    }

    private void readEntriesWithAcquiredPermits(ReadHandle lh, long firstEntry, long lastEntry,
                                                AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                                InflightReadsLimiter.Handle handle) {
        if (!handle.success()) {
            long estimatedReadSize = (lastEntry - firstEntry + 1) * getEstimatedEntrySize(lh);
            String message = String.format(
                    "Couldn't acquire enough permits on the max reads in flight limiter to read from ledger "
                            + "%d, %s, estimated read size %d bytes for %d entries (check "
                            + "managedLedgerMaxReadsInFlightPermitsAcquireQueueSize (direct config), "
                            + "managedLedgerMaxReadsInFlightPermitsAcquireTimeoutMillis and "
                            + "managedLedgerMaxReadsInFlightSizeInMB)", lh.getId(), getName(), estimatedReadSize,
                    (int) (lastEntry - firstEntry + 1));
            callback.readEntriesFailed(new ManagedLedgerException.TooManyRequestsException(message), ctx);
            return;
        }
        readEntries(lh, firstEntry, lastEntry, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object callbackCtx) {
                if (entries.isEmpty()) {
                    inflightReadsLimiter.release(handle);
                } else {
                    AtomicInteger remainingEntries = new AtomicInteger(entries.size());
                    for (Entry entry : entries) {
                        ((EntryImpl) entry).onDeallocate(() -> {
                            if (remainingEntries.decrementAndGet() == 0) {
                                inflightReadsLimiter.release(handle);
                            }
                        });
                    }
                }
                callback.readEntriesComplete(entries, callbackCtx);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object callbackCtx) {
                inflightReadsLimiter.release(handle);
                callback.readEntriesFailed(exception, callbackCtx);
            }
        }, ctx);
    }

    private void readEntries(ReadHandle lh, long firstEntry, long lastEntry,
                             AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        ReadEntryUtils.readAsync(ml, lh, firstEntry, lastEntry).thenApplyAsync(
                ledgerEntries -> {
                    List<Entry> entries = new ArrayList<>();
                    long totalSize = 0;
                    try {
                        for (LedgerEntry e : ledgerEntries) {
                            // Insert the entries at the end of the list (they will be unsorted for now)
                            EntryImpl entry = EntryImpl.create(e, interceptor, 0);
                            entry.initializeMessageMetadataIfNeeded(ml.getName());
                            entries.add(entry);
                            totalSize += entry.getLength();
                        }
                    } finally {
                        ledgerEntries.close();
                    }
                    ml.getMbean().recordReadEntriesOpsCacheMisses(entries.size(), totalSize);
                    ml.getFactory().getMbean().recordCacheMiss(entries.size(), totalSize);
                    ml.getMbean().addReadEntriesSample(entries.size(), totalSize);

                    return entries;
                }, ml.getExecutor()).whenCompleteAsync((entries, exception) -> {
                    if (exception == null) {
                        callback.readEntriesComplete(entries, ctx);
                    } else {
                        callback.readEntriesFailed(createManagedLedgerException(exception), ctx);
                    }
                }, ml.getExecutor());
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, Position position, AsyncCallbacks.ReadEntryCallback callback,
                               Object ctx) {
        asyncReadEntry(lh, position.getEntryId(), position.getEntryId(), () -> 0,
                new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object callbackCtx) {
                        Iterator<Entry> iterator = entries.iterator();
                        if (iterator.hasNext()) {
                            callback.readEntryComplete(iterator.next(), callbackCtx);
                        } else {
                            callback.readEntryFailed(new ManagedLedgerException("Could not read given position"),
                                    callbackCtx);
                        }
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object callbackCtx) {
                        if (!(exception instanceof ManagedLedgerException.TooManyRequestsException)) {
                            ml.invalidateLedgerHandle(lh);
                        }
                        callback.readEntryFailed(exception, callbackCtx);
                    }
                }, ctx);
    }

    @Override
    public long getSize() {
        return 0;
    }

    private static long getEstimatedEntrySize(ReadHandle lh) {
        if (lh.getLength() == 0 || lh.getLastAddConfirmed() < 0) {
            return RangeEntryCacheImpl.DEFAULT_ESTIMATED_ENTRY_SIZE
                    + RangeEntryCacheImpl.BOOKKEEPER_READ_OVERHEAD_PER_ENTRY;
        }
        return Math.max(1, lh.getLength() / (lh.getLastAddConfirmed() + 1))
                + RangeEntryCacheImpl.BOOKKEEPER_READ_OVERHEAD_PER_ENTRY;
    }
}

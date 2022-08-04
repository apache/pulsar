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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
class SharedEntryCacheImpl implements EntryCache {

    private final SharedEntryCacheManagerImpl entryCacheManager;
    private final ManagedLedgerImpl ml;
    private final ManagedLedgerInterceptor interceptor;

    SharedEntryCacheImpl(ManagedLedgerImpl ml, SharedEntryCacheManagerImpl entryCacheManager) {
        this.ml = ml;
        this.entryCacheManager = entryCacheManager;
        this.interceptor = ml.getManagedLedgerInterceptor();
    }

    @Override
    public String getName() {
        return ml.getName();
    }

    @Override
    public boolean insert(EntryImpl entry) {
        return entryCacheManager.insert(entry);
    }

    @Override
    public void invalidateEntries(PositionImpl lastPosition) {
        // No-Op. The cache invalidation is based only on rotating the segment buffers
    }

    @Override
    public void invalidateEntriesBeforeTimestamp(long timestamp) {
        // No-Op. The cache invalidation is based only on rotating the segment buffers
    }

    @Override
    public void invalidateAllEntries(long ledgerId) {
        // No-Op. The cache invalidation is based only on rotating the segment buffers
    }

    @Override
    public void clear() {
        // No-Op. The cache invalidation is based only on rotating the segment buffers
    }

    private static final Pair<Integer, Long> NO_EVICTION = Pair.of(0, 0L);

    @Override
    public Pair<Integer, Long> evictEntries(long sizeToFree) {
        return NO_EVICTION;
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
                               AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        final long ledgerId = lh.getId();
        final int entriesToRead = (int) (lastEntry - firstEntry) + 1;

        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entries range ledger {}: {} to {}", ml.getName(), ledgerId, firstEntry, lastEntry);
        }

        List<Entry> cachedEntries = new ArrayList<>(entriesToRead);
        long totalCachedSize = entryCacheManager.getRange(ledgerId, firstEntry, lastEntry, cachedEntries);

        if (cachedEntries.size() == entriesToRead) {
            final List<Entry> entriesToReturn = Lists.newArrayListWithExpectedSize(entriesToRead);
            // All entries found in cache
            for (Entry entry : cachedEntries) {
                entriesToReturn.add(EntryImpl.create((EntryImpl) entry));
                entry.release();
            }
            // All entries found in cache
            entryCacheManager.getFactoryMBean().recordCacheHits(entriesToReturn.size(), totalCachedSize);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ledger {} -- Found in cache entries: {}-{}", ml.getName(), ledgerId, firstEntry,
                        lastEntry);
            }
            callback.readEntriesComplete(entriesToReturn, ctx);

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
                            final List<Entry> entriesToReturn = Lists.newArrayListWithExpectedSize(entriesToRead);
                            for (LedgerEntry e : ledgerEntries) {
                                EntryImpl entry = EntryCacheManager.create(e, interceptor);

                                entriesToReturn.add(entry);
                                totalSize += entry.getLength();
                            }

                            entryCacheManager.getFactoryMBean().recordCacheMiss(entriesToReturn.size(), totalSize);
                            ml.getMbean().addReadEntriesSample(entriesToReturn.size(), totalSize);

                            callback.readEntriesComplete(entriesToReturn, ctx);
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
    public void asyncReadEntry(ReadHandle lh, PositionImpl position, AsyncCallbacks.ReadEntryCallback callback,
                               Object ctx) {
        try {
            asyncReadEntry0(lh, position, callback, ctx);
        } catch (Throwable t) {
            log.warn("[{}] Failed to read entries for {}-{}", getName(), lh.getId(), position, t);
            callback.readEntryFailed(createManagedLedgerException(t), ctx);
        }
    }

    private void asyncReadEntry0(ReadHandle lh, PositionImpl position, AsyncCallbacks.ReadEntryCallback callback,
                                 Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entry ledger {}: {}", ml.getName(), lh.getId(), position.getEntryId());
        }

        EntryImpl cachedEntry = entryCacheManager.get(position.getLedgerId(), position.getEntryId());

        if (cachedEntry != null) {
            EntryImpl entry = EntryImpl.create(cachedEntry);
            cachedEntry.release();
            entryCacheManager.getFactoryMBean().recordCacheHit(entry.getLength());
            callback.readEntryComplete(entry, ctx);
        } else {
            lh.readAsync(position.getEntryId(), position.getEntryId()).thenAcceptAsync(
                    ledgerEntries -> {
                        try {
                            Iterator<LedgerEntry> iterator = ledgerEntries.iterator();
                            if (iterator.hasNext()) {
                                LedgerEntry ledgerEntry = iterator.next();
                                EntryImpl returnEntry = EntryCacheManager.create(ledgerEntry, interceptor);

                                entryCacheManager.getFactoryMBean().recordCacheMiss(1, returnEntry.getLength());
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
    public long getSize() {
        return 0;
    }

    @Override
    public int compareTo(EntryCache o) {
        // The individual topic caches cannot be compared since the cache is shared
        return 0;
    }
}

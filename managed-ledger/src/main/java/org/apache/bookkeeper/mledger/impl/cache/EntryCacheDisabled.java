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

import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.createManagedLedgerException;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import java.util.Iterator;
import java.util.List;
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

/**
 * Implementation of cache that always read from BookKeeper.
 */
public class EntryCacheDisabled implements EntryCache {
    private final ManagedLedgerImpl ml;
    private final ManagedLedgerInterceptor interceptor;

    public EntryCacheDisabled(ManagedLedgerImpl ml) {
        this.ml = ml;
        this.interceptor = ml.getManagedLedgerInterceptor();
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
        return Pair.of(0, (long) 0);
    }

    @Override
    public void invalidateEntriesBeforeTimestamp(long timestamp) {
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry, boolean isSlowestReader,
                               final AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        lh.readAsync(firstEntry, lastEntry).thenAcceptAsync(
                ledgerEntries -> {
                    List<Entry> entries = Lists.newArrayList();
                    long totalSize = 0;
                    try {
                        for (LedgerEntry e : ledgerEntries) {
                            // Insert the entries at the end of the list (they will be unsorted for now)
                            EntryImpl entry = RangeEntryCacheManagerImpl.create(e, interceptor);
                            entries.add(entry);
                            totalSize += entry.getLength();
                        }
                    } finally {
                        ledgerEntries.close();
                    }
                    ml.getFactory().getMbean().recordCacheMiss(entries.size(), totalSize);
                    ml.getMbean().addReadEntriesSample(entries.size(), totalSize);

                    callback.readEntriesComplete(entries, ctx);
                }, ml.getExecutor().chooseThread(ml.getName())).exceptionally(exception -> {
            callback.readEntriesFailed(createManagedLedgerException(exception), ctx);
            return null;
        });
    }

    @Override
    public void asyncReadEntry(ReadHandle lh, PositionImpl position, AsyncCallbacks.ReadEntryCallback callback,
                               Object ctx) {
        lh.readAsync(position.getEntryId(), position.getEntryId()).whenCompleteAsync(
                (ledgerEntries, exception) -> {
                    if (exception != null) {
                        ml.invalidateLedgerHandle(lh);
                        callback.readEntryFailed(createManagedLedgerException(exception), ctx);
                        return;
                    }

                    try {
                        Iterator<LedgerEntry> iterator = ledgerEntries.iterator();
                        if (iterator.hasNext()) {
                            LedgerEntry ledgerEntry = iterator.next();
                            EntryImpl returnEntry = RangeEntryCacheManagerImpl.create(ledgerEntry, interceptor);

                            ml.getFactory().getMbean().recordCacheMiss(1, returnEntry.getLength());
                            ml.getMbean().addReadEntriesSample(1, returnEntry.getLength());
                            callback.readEntryComplete(returnEntry, ctx);
                        } else {
                            callback.readEntryFailed(new ManagedLedgerException("Could not read given position"),
                                    ctx);
                        }
                    } finally {
                        ledgerEntries.close();
                    }
                }, ml.getExecutor().chooseThread(ml.getName()));
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

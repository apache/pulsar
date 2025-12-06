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
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
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

    public EntryCacheDisabled(ManagedLedgerImpl ml) {
        this.ml = ml;
        this.interceptor = ml.getManagedLedgerInterceptor();
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
    public CompletableFuture<List<Entry>> asyncReadEntry(ReadHandle lh, long firstEntry, long lastEntry,
                                                         IntSupplier expectedReadCount) {
        return ReadEntryUtils.readAsync(ml, lh, firstEntry, lastEntry).thenApplyAsync(
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
                }, ml.getExecutor());
    }

    @Override
    public CompletableFuture<Entry> asyncReadEntry(ReadHandle lh, Position position) {
        final var future = new CompletableFuture<Entry>();
        ReadEntryUtils.readAsync(ml, lh, position.getEntryId(), position.getEntryId()).whenCompleteAsync(
                (ledgerEntries, exception) -> {
                    if (exception != null) {
                        ml.invalidateLedgerHandle(lh);
                        future.completeExceptionally(createManagedLedgerException(exception));
                        return;
                    }

                    try {
                        Iterator<LedgerEntry> iterator = ledgerEntries.iterator();
                        if (iterator.hasNext()) {
                            LedgerEntry ledgerEntry = iterator.next();
                            EntryImpl returnEntry = EntryImpl.create(ledgerEntry, interceptor, 0);
                            returnEntry.initializeMessageMetadataIfNeeded(ml.getName());
                            ml.getMbean().recordReadEntriesOpsCacheMisses(1, returnEntry.getLength());
                            ml.getFactory().getMbean().recordCacheMiss(1, returnEntry.getLength());
                            ml.getMbean().addReadEntriesSample(1, returnEntry.getLength());
                            future.complete(returnEntry);
                        } else {
                            future.completeExceptionally(new ManagedLedgerException("Could not read given position"));
                        }
                    } finally {
                        ledgerEntries.close();
                    }
                }, ml.getExecutor());
        return future;
    }

    @Override
    public long getSize() {
        return 0;
    }
}

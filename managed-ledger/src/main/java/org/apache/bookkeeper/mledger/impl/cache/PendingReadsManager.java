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
import io.prometheus.client.Counter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.EntryImpl;

/**
 * PendingReadsManager tries to prevent sending duplicate reads to BK.
 */
@Slf4j
public class PendingReadsManager {

    private static final Counter COUNT_ENTRIES_READ_FROM_BK = Counter
            .build()
            .name("pulsar_ml_cache_pendingreads_entries_read")
            .help("Total number of entries read from BK")
            .register();

    private static final Counter COUNT_ENTRIES_NOTREAD_FROM_BK = Counter
            .build()
            .name("pulsar_ml_cache_pendingreads_entries_notread")
            .help("Total number of entries not read from BK")
            .register();

    private static final Counter COUNT_PENDING_READS_MATCHED = Counter
            .build()
            .name("pulsar_ml_cache_pendingreads_matched")
            .help("Pending reads reused with perfect range match")
            .register();
    private static final Counter COUNT_PENDING_READS_MATCHED_INCLUDED = Counter
            .build()
            .name("pulsar_ml_cache_pendingreads_matched_included")
            .help("Pending reads reused by attaching to a read with a larger range")
            .register();
    private static final Counter COUNT_PENDING_READS_MISSED = Counter
            .build()
            .name("pulsar_ml_cache_pendingreads_missed")
            .help("Pending reads that didn't find a match")
            .register();

    private static final Counter COUNT_PENDING_READS_MATCHED_OVERLAPPING_MISS_LEFT = Counter
            .build()
            .name("pulsar_ml_cache_pendingreads_matched_overlapping_miss_left")
            .help("Pending reads that didn't find a match but they partially overlap with another read")
            .register();

    private static final Counter COUNT_PENDING_READS_MATCHED_BUT_OVERLAPPING_MISS_RIGHT = Counter
            .build()
            .name("pulsar_ml_cache_pendingreads_matched_overlapping_miss_right")
            .help("Pending reads that didn't find a match but they partially overlap with another read")
            .register();

    private static final Counter COUNT_PENDING_READS_MATCHED_BUT_OVERLAPPING_MISS_BOTH = Counter
            .build()
            .name("pulsar_ml_cache_pendingreads_matched_overlapping_miss_both")
            .help("Pending reads that didn't find a match but they partially overlap with another read")
            .register();

    private final RangeEntryCacheImpl rangeEntryCache;
    private final ConcurrentHashMap<Long, ConcurrentHashMap<PendingReadKey, PendingRead>> cachedPendingReads =
            new ConcurrentHashMap<>();

    public PendingReadsManager(RangeEntryCacheImpl rangeEntryCache) {
        this.rangeEntryCache = rangeEntryCache;
    }

    @Value
    private static class PendingReadKey {
        private final long startEntry;
        private final long endEntry;
        long size() {
            return endEntry - startEntry + 1;
        }


        boolean includes(PendingReadKey other) {
            return startEntry <= other.startEntry && other.endEntry <= endEntry;
        }

        boolean overlaps(PendingReadKey other) {
            return (other.startEntry <= startEntry && startEntry <= other.endEntry)
                    || (other.startEntry <= endEntry && endEntry <= other.endEntry);
        }

        PendingReadKey reminderOnLeft(PendingReadKey other) {
            //   S******-----E
            //          S----------E
            if (other.startEntry <= endEntry
                    && other.startEntry > startEntry) {
                return new PendingReadKey(startEntry, other.startEntry - 1);
            }
            return null;
        }

        PendingReadKey reminderOnRight(PendingReadKey other) {
            //          S-----*******E
            //   S-----------E
            if (startEntry <= other.endEntry
                    && other.endEntry < endEntry) {
                return new PendingReadKey(other.endEntry + 1, endEntry);
            }
            return null;
        }

    }

    @AllArgsConstructor
    private static final class ReadEntriesCallbackWithContext {
        final AsyncCallbacks.ReadEntriesCallback callback;
        final Object ctx;
        final long startEntry;
        final long endEntry;
    }

    @AllArgsConstructor
    private static final class FindPendingReadOutcome {
        final PendingRead pendingRead;
        final PendingReadKey missingOnLeft;
        final PendingReadKey missingOnRight;
        boolean needsAdditionalReads() {
            return missingOnLeft != null || missingOnRight != null;
        }
    }

    private FindPendingReadOutcome findPendingRead(PendingReadKey key, Map<PendingReadKey,
            PendingRead> ledgerCache, AtomicBoolean created) {
        synchronized (ledgerCache) {
            PendingRead existing = ledgerCache.get(key);
            if (existing != null) {
                COUNT_PENDING_READS_MATCHED.inc(key.size());
                COUNT_ENTRIES_NOTREAD_FROM_BK.inc(key.size());
                return new FindPendingReadOutcome(existing, null, null);
            }
            FindPendingReadOutcome foundButMissingSomethingOnLeft = null;
            FindPendingReadOutcome foundButMissingSomethingOnRight = null;
            FindPendingReadOutcome foundButMissingSomethingOnBoth = null;

            for (Map.Entry<PendingReadKey, PendingRead> entry : ledgerCache.entrySet()) {
                PendingReadKey entryKey = entry.getKey();
                if (entryKey.includes(key)) {
                    COUNT_PENDING_READS_MATCHED_INCLUDED.inc(key.size());
                    COUNT_ENTRIES_NOTREAD_FROM_BK.inc(key.size());
                    return new FindPendingReadOutcome(entry.getValue(), null, null);
                }
                if (entryKey.overlaps(key)) {
                    PendingReadKey reminderOnLeft = key.reminderOnLeft(entryKey);
                    PendingReadKey reminderOnRight = key.reminderOnRight(entryKey);
                    if (reminderOnLeft != null && reminderOnRight != null) {
                        foundButMissingSomethingOnBoth = new FindPendingReadOutcome(entry.getValue(),
                                reminderOnLeft, reminderOnRight);
                    } else if (reminderOnRight != null && reminderOnLeft == null) {
                        foundButMissingSomethingOnRight = new FindPendingReadOutcome(entry.getValue(),
                                null, reminderOnRight);
                    } else if (reminderOnLeft != null && reminderOnRight == null) {
                        foundButMissingSomethingOnLeft = new FindPendingReadOutcome(entry.getValue(),
                                reminderOnLeft, null);
                    }
                }
            }

            if (foundButMissingSomethingOnRight != null) {
                long delta = key.size()
                        - foundButMissingSomethingOnRight.missingOnRight.size();
                COUNT_PENDING_READS_MATCHED_BUT_OVERLAPPING_MISS_RIGHT.inc(delta);
                COUNT_ENTRIES_NOTREAD_FROM_BK.inc(delta);
                return foundButMissingSomethingOnRight;
            } else if (foundButMissingSomethingOnLeft != null) {
                long delta = key.size()
                        - foundButMissingSomethingOnLeft.missingOnLeft.size();
                COUNT_PENDING_READS_MATCHED_OVERLAPPING_MISS_LEFT.inc(delta);
                COUNT_ENTRIES_NOTREAD_FROM_BK.inc(delta);
                return foundButMissingSomethingOnLeft;
            } else if (foundButMissingSomethingOnBoth != null) {
                long delta = key.size()
                        - foundButMissingSomethingOnBoth.missingOnRight.size()
                        - foundButMissingSomethingOnBoth.missingOnLeft.size();
                COUNT_ENTRIES_NOTREAD_FROM_BK.inc(delta);
                COUNT_PENDING_READS_MATCHED_BUT_OVERLAPPING_MISS_BOTH.inc(delta);
                return foundButMissingSomethingOnBoth;
            }

            created.set(true);
            PendingRead newRead = new PendingRead(key, ledgerCache);
            ledgerCache.put(key, newRead);
            long delta = key.size();
            COUNT_PENDING_READS_MISSED.inc(delta);
            COUNT_ENTRIES_READ_FROM_BK.inc(delta);
            return new FindPendingReadOutcome(newRead, null, null);
        }
    }

    private class PendingRead {
        final PendingReadKey key;
        final Map<PendingReadKey, PendingRead> ledgerCache;
        final List<ReadEntriesCallbackWithContext> callbacks = new ArrayList<>(1);
        boolean completed = false;

        public PendingRead(PendingReadKey key,
                           Map<PendingReadKey, PendingRead> ledgerCache) {
            this.key = key;
            this.ledgerCache = ledgerCache;
        }

        private List<EntryImpl> keepEntries(List<EntryImpl> list, long startEntry, long endEntry) {
            List<EntryImpl> result = new ArrayList<>((int) (endEntry - startEntry));
            for (EntryImpl entry : list) {
                long entryId = entry.getEntryId();
                if (startEntry <= entryId && entryId <= endEntry) {
                    result.add(entry);
                } else {
                    entry.release();
                }
            }
            return result;
        }

        public void attach(CompletableFuture<List<EntryImpl>> handle) {
            // when the future is done remove this from the map
            // new reads will go to a new instance
            // this is required because we are going to do refcount management
            // on the results of the callback
            handle.whenComplete((___, error) -> {
                synchronized (PendingRead.this) {
                    completed = true;
                    synchronized (ledgerCache) {
                        ledgerCache.remove(key, this);
                    }
                }
            });

            handle.thenAcceptAsync(entriesToReturn -> {
                synchronized (PendingRead.this) {
                    if (callbacks.size() == 1) {
                        ReadEntriesCallbackWithContext first = callbacks.get(0);
                        if (first.startEntry == key.startEntry
                                && first.endEntry == key.endEntry) {
                            // perfect match, no copy, this is the most common case
                            first.callback.readEntriesComplete((List) entriesToReturn,
                                    first.ctx);
                        } else {
                            first.callback.readEntriesComplete(
                                    (List) keepEntries(entriesToReturn, first.startEntry, first.endEntry),
                                    first.ctx);
                        }
                    } else {
                        for (ReadEntriesCallbackWithContext callback : callbacks) {
                            long callbackStartEntry = callback.startEntry;
                            long callbackEndEntry = callback.endEntry;
                            List<EntryImpl> copy = new ArrayList<>((int) (callbackEndEntry - callbackStartEntry + 1));
                            for (EntryImpl entry : entriesToReturn) {
                                long entryId = entry.getEntryId();
                                if (callbackStartEntry <= entryId && entryId <= callbackEndEntry) {
                                    EntryImpl entryCopy = EntryImpl.create(entry);
                                    copy.add(entryCopy);
                                }
                            }
                            callback.callback.readEntriesComplete((List) copy, callback.ctx);
                        }
                        for (EntryImpl entry : entriesToReturn) {
                            entry.release();
                        }
                    }
                }
            }, rangeEntryCache.getManagedLedger().getExecutor()).exceptionally(exception -> {
                synchronized (PendingRead.this) {
                    for (ReadEntriesCallbackWithContext callback : callbacks) {
                        ManagedLedgerException mlException = createManagedLedgerException(exception);
                        callback.callback.readEntriesFailed(mlException, callback.ctx);
                    }
                }
                return null;
            });
        }

        synchronized boolean addListener(AsyncCallbacks.ReadEntriesCallback callback,
                                         Object ctx, long startEntry, long endEntry) {
            if (completed) {
                return false;
            }
            callbacks.add(new ReadEntriesCallbackWithContext(callback, ctx, startEntry, endEntry));
            return true;
        }
    }


    void readEntries(ReadHandle lh, long firstEntry, long lastEntry, boolean shouldCacheEntry,
                     final AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        final PendingReadKey key = new PendingReadKey(firstEntry, lastEntry);

        Map<PendingReadKey, PendingRead> pendingReadsForLedger =
                cachedPendingReads.computeIfAbsent(lh.getId(), (l) -> new ConcurrentHashMap<>());

        boolean listenerAdded = false;
        while (!listenerAdded) {
            AtomicBoolean createdByThisThread = new AtomicBoolean();
            FindPendingReadOutcome findBestCandidateOutcome = findPendingRead(key,
                    pendingReadsForLedger, createdByThisThread);
            PendingRead pendingRead = findBestCandidateOutcome.pendingRead;
            if (findBestCandidateOutcome.needsAdditionalReads()) {
                AsyncCallbacks.ReadEntriesCallback wrappedCallback = new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        PendingReadKey missingOnLeft = findBestCandidateOutcome.missingOnLeft;
                        PendingReadKey missingOnRight = findBestCandidateOutcome.missingOnRight;
                        if (missingOnRight != null && missingOnLeft != null) {
                            AsyncCallbacks.ReadEntriesCallback readFromLeftCallback =
                                    new AsyncCallbacks.ReadEntriesCallback() {
                                @Override
                                public void readEntriesComplete(List<Entry> entriesFromLeft, Object dummyCtx1) {
                                    AsyncCallbacks.ReadEntriesCallback readFromRightCallback =
                                            new AsyncCallbacks.ReadEntriesCallback() {
                                        @Override
                                        public void readEntriesComplete(List<Entry> entriesFromRight,
                                                                        Object dummyCtx2) {
                                            List<Entry> finalResult =
                                                    new ArrayList<>(entriesFromLeft.size()
                                                            + entries.size() + entriesFromRight.size());
                                            finalResult.addAll(entriesFromLeft);
                                            finalResult.addAll(entries);
                                            finalResult.addAll(entriesFromRight);
                                            callback.readEntriesComplete(finalResult, ctx);
                                        }

                                        @Override
                                        public void readEntriesFailed(ManagedLedgerException exception,
                                                                      Object dummyCtx3) {
                                            entries.forEach(Entry::release);
                                            entriesFromLeft.forEach(Entry::release);
                                            callback.readEntriesFailed(exception, ctx);
                                        }
                                    };
                                    rangeEntryCache.asyncReadEntry0(lh,
                                            missingOnRight.startEntry, missingOnRight.endEntry,
                                            shouldCacheEntry, readFromRightCallback, null, false);
                                }

                                @Override
                                public void readEntriesFailed(ManagedLedgerException exception, Object dummyCtx4) {
                                    entries.forEach(Entry::release);
                                    callback.readEntriesFailed(exception, ctx);
                                }
                            };
                            rangeEntryCache.asyncReadEntry0(lh, missingOnLeft.startEntry, missingOnLeft.endEntry,
                                    shouldCacheEntry, readFromLeftCallback, null, false);
                        } else if (missingOnLeft != null) {
                            AsyncCallbacks.ReadEntriesCallback readFromLeftCallback =
                                    new AsyncCallbacks.ReadEntriesCallback() {

                                        @Override
                                        public void readEntriesComplete(List<Entry> entriesFromLeft,
                                                                        Object dummyCtx5) {
                                            List<Entry> finalResult =
                                                    new ArrayList<>(entriesFromLeft.size() + entries.size());
                                            finalResult.addAll(entriesFromLeft);
                                            finalResult.addAll(entries);
                                            callback.readEntriesComplete(finalResult, ctx);
                                        }

                                        @Override
                                        public void readEntriesFailed(ManagedLedgerException exception,
                                                                      Object dummyCtx6) {
                                            entries.forEach(Entry::release);
                                            callback.readEntriesFailed(exception, ctx);
                                        }
                                    };
                            rangeEntryCache.asyncReadEntry0(lh, missingOnLeft.startEntry, missingOnLeft.endEntry,
                                    shouldCacheEntry, readFromLeftCallback, null, false);
                        } else if (missingOnRight != null) {
                            AsyncCallbacks.ReadEntriesCallback readFromRightCallback =
                                    new AsyncCallbacks.ReadEntriesCallback() {

                                        @Override
                                        public void readEntriesComplete(List<Entry> entriesFromRight,
                                                                        Object dummyCtx7) {
                                            List<Entry> finalResult =
                                                    new ArrayList<>(entriesFromRight.size() + entries.size());
                                            finalResult.addAll(entries);
                                            finalResult.addAll(entriesFromRight);
                                            callback.readEntriesComplete(finalResult, ctx);
                                        }

                                        @Override
                                        public void readEntriesFailed(ManagedLedgerException exception,
                                                                      Object dummyCtx8) {
                                            entries.forEach(Entry::release);
                                            callback.readEntriesFailed(exception, ctx);
                                        }
                                    };
                            rangeEntryCache.asyncReadEntry0(lh, missingOnRight.startEntry, missingOnRight.endEntry,
                                    shouldCacheEntry, readFromRightCallback, null, false);
                        }
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        callback.readEntriesFailed(exception, ctx);
                    }
                };
                listenerAdded = pendingRead.addListener(wrappedCallback, ctx, key.startEntry, key.endEntry);
            } else {
                listenerAdded = pendingRead.addListener(callback, ctx, key.startEntry, key.endEntry);
            }


            if (createdByThisThread.get()) {
                CompletableFuture<List<EntryImpl>> readResult = rangeEntryCache.readFromStorage(lh, firstEntry,
                        lastEntry, shouldCacheEntry);
                pendingRead.attach(readResult);
            }
        }
    }


    void clear() {
        cachedPendingReads.clear();
    }

    void invalidateLedger(long id) {
        cachedPendingReads.remove(id);
    }
}

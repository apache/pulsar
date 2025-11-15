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
import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.Counter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.ReadHandle;
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

    private record PendingReadKey(long startEntry, long endEntry) {
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

    private record ReadEntriesCallbackWithContext(CompletableFuture<List<Entry>> future,
                                                  long startEntry, long endEntry) {
    }

    private record FindPendingReadOutcome(PendingRead pendingRead,
                                          PendingReadKey missingOnLeft, PendingReadKey missingOnRight) {
        boolean needsAdditionalReads() {
            return missingOnLeft != null || missingOnRight != null;
        }
    }

    private FindPendingReadOutcome findPendingRead(PendingReadKey key, ConcurrentMap<PendingReadKey,
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
        final ConcurrentMap<PendingReadKey, PendingRead> ledgerCache;
        final List<ReadEntriesCallbackWithContext> listeners = new ArrayList<>(1);
        PendingReadState state = PendingReadState.INITIALISED;

        enum PendingReadState {
            INITIALISED,
            ATTACHED,
            COMPLETED
        }

        public PendingRead(PendingReadKey key,
                           ConcurrentMap<PendingReadKey, PendingRead> ledgerCache) {
            this.key = key;
            this.ledgerCache = ledgerCache;
        }

        public synchronized void attach(CompletableFuture<List<Entry>> handle) {
            if (state != PendingReadState.INITIALISED) {
                // this shouldn't ever happen. this is here to prevent misuse in future changes
                throw new IllegalStateException("Unexpected state " + state + " for PendingRead for key " + key);
            }
            state = PendingReadState.ATTACHED;
            handle.whenComplete((entriesToReturn, error) -> {
                // execute in the completing thread and return a copy of the listeners
                List<ReadEntriesCallbackWithContext> callbacks = completeAndRemoveFromCache();
                // execute the callbacks in the managed ledger executor
                rangeEntryCache.getManagedLedger().getExecutor().execute(() -> {
                    if (error != null) {
                        readEntriesFailed(callbacks, error);
                    } else {
                        readEntriesComplete(callbacks, entriesToReturn);
                    }
                });
            });
        }

        synchronized boolean addListener(CompletableFuture<List<Entry>> entriesFuture, long startEntry, long endEntry) {
            if (state == PendingReadState.COMPLETED) {
                return false;
            }
            listeners.add(new ReadEntriesCallbackWithContext(entriesFuture, startEntry, endEntry));
            return true;
        }

        private synchronized List<ReadEntriesCallbackWithContext> completeAndRemoveFromCache() {
            state = PendingReadState.COMPLETED;
            // When the read has completed, remove the instance from the ledgerCache map
            // so that new reads will go to a new instance.
            // this is required because we are going to do refcount management
            // on the results of the callback
            ledgerCache.remove(key, this);
            // return a copy of the listeners
            return List.copyOf(listeners);
        }

        // this method isn't synchronized since that could lead to deadlocks
        private void readEntriesComplete(List<ReadEntriesCallbackWithContext> callbacks,
                                         List<Entry> entriesToReturn) {
            if (callbacks.size() == 1) {
                ReadEntriesCallbackWithContext first = callbacks.get(0);
                if (first.startEntry == key.startEntry
                        && first.endEntry == key.endEntry) {
                    // perfect match, no copy, this is the most common case
                    first.future.complete(entriesToReturn);
                } else {
                    first.future.complete(keepEntries(entriesToReturn, first.startEntry, first.endEntry));
                }
            } else {
                for (ReadEntriesCallbackWithContext callback : callbacks) {
                    callback.future.complete(copyEntries(entriesToReturn, callback.startEntry, callback.endEntry));
                }
                for (Entry entry : entriesToReturn) {
                    // don't decrease the read count when these entries are released
                    ((EntryImpl) entry).setDecreaseReadCountOnRelease(false);
                    entry.release();
                }
            }
        }

        // this method isn't synchronized since that could lead to deadlocks
        private void readEntriesFailed(List<ReadEntriesCallbackWithContext> callbacks, Throwable error) {
            for (ReadEntriesCallbackWithContext callback : callbacks) {
                ManagedLedgerException mlException = createManagedLedgerException(error);
                callback.future.completeExceptionally(mlException);
            }
        }

        private static List<Entry> keepEntries(List<Entry> list, long startEntry, long endEntry) {
            List<Entry> result = new ArrayList<>((int) (endEntry - startEntry + 1));
            for (Entry entry : list) {
                long entryId = entry.getEntryId();
                if (startEntry <= entryId && entryId <= endEntry) {
                    result.add(entry);
                } else {
                    entry.release();
                }
            }
            return result;
        }

        private static List<Entry> copyEntries(List<Entry> entriesToReturn, long startEntry, long endEntry) {
            List<Entry> result = new ArrayList<>((int) (endEntry - startEntry + 1));
            for (Entry entry : entriesToReturn) {
                long entryId = entry.getEntryId();
                if (startEntry <= entryId && entryId <= endEntry) {
                    EntryImpl entryCopy = EntryImpl.create(entry);
                    result.add(entryCopy);
                }
            }
            return result;
        }
    }

    @VisibleForTesting
    CompletableFuture<List<Entry>> readEntries(ReadHandle lh, long firstEntry, long lastEntry,
                                               IntSupplier expectedReadCount) {
        final var readFuture = new CompletableFuture<List<Entry>>();
        final PendingReadKey key = new PendingReadKey(firstEntry, lastEntry);

        ConcurrentMap<PendingReadKey, PendingRead> pendingReadsForLedger =
                cachedPendingReads.computeIfAbsent(lh.getId(), (l) -> new ConcurrentHashMap<>());

        boolean listenerAdded = false;
        while (!listenerAdded) {
            AtomicBoolean createdByThisThread = new AtomicBoolean();
            FindPendingReadOutcome findBestCandidateOutcome = findPendingRead(key,
                    pendingReadsForLedger, createdByThisThread);
            PendingRead pendingRead = findBestCandidateOutcome.pendingRead;

            if (findBestCandidateOutcome.needsAdditionalReads()) {
                CompletableFuture<List<Entry>> readFromMidFuture = new CompletableFuture<>();
                listenerAdded = pendingRead.addListener(readFromMidFuture, key.startEntry, key.endEntry);
                if (!listenerAdded) {
                    continue;
                }
                CompletableFuture<List<Entry>> readFromLeftFuture =
                        recursiveReadMissingEntriesAsync(lh, expectedReadCount, findBestCandidateOutcome.missingOnLeft);
                CompletableFuture<List<Entry>> readFromRightFuture =
                        recursiveReadMissingEntriesAsync(lh, expectedReadCount,
                                findBestCandidateOutcome.missingOnRight);
                readFromLeftFuture
                        .thenCombine(readFromMidFuture, (left, mid) -> {
                            List<Entry> result = new ArrayList<>(left);
                            result.addAll(mid);
                            return result;
                        })
                        .thenCombine(readFromRightFuture, (combined, right) -> {
                            combined.addAll(right);
                            return combined;
                        })
                        .whenComplete((finalResult, e) -> {
                            if (e != null) {
                                readFuture.completeExceptionally(e);
                                releaseEntriesSafely(readFromLeftFuture);
                                releaseEntriesSafely(readFromMidFuture);
                                releaseEntriesSafely(readFromRightFuture);
                            } else {
                                readFuture.complete(finalResult);
                            }
                        });
            } else {
                listenerAdded = pendingRead.addListener(readFuture, key.startEntry, key.endEntry);
            }

            if (createdByThisThread.get()) {
                CompletableFuture<List<Entry>> readResult = rangeEntryCache.readFromStorage(lh, firstEntry,
                        lastEntry, expectedReadCount);
                pendingRead.attach(readResult);
            }
        }
        return readFuture;
    }

    private CompletableFuture<List<Entry>> recursiveReadMissingEntriesAsync(ReadHandle lh,
                                                                            IntSupplier expectedReadCount,
                                                                            PendingReadKey missingKey) {
        if (missingKey != null) {
            return rangeEntryCache.asyncReadEntry0(lh, missingKey.startEntry, missingKey.endEntry, expectedReadCount,
                    false);
        } else {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    private void releaseEntriesSafely(CompletableFuture<List<Entry>> future) {
        if (!future.isCompletedExceptionally()) {
            future.thenAccept(entries -> entries.forEach(Entry::release));
        }
    }


    void clear() {
        cachedPendingReads.clear();
    }

    void invalidateLedger(long id) {
        cachedPendingReads.remove(id);
    }

}

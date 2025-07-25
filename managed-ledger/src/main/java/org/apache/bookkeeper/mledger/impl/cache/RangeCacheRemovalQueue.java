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
import org.apache.commons.lang3.tuple.Pair;
import org.jctools.queues.MpscUnboundedArrayQueue;

/**
 * A central queue to hold entries that are scheduled for removal from all range cache instances.
 * Removal of entries is done in a single thread to avoid contention.
 * This queue is used to evict entries based on timestamp or based on size to free up space in the cache.
 */
class RangeCacheRemovalQueue {
    // The removal queue is unbounded, but we allocate memory in chunks to avoid frequent memory allocations.
    private static final int REMOVAL_QUEUE_CHUNK_SIZE = 128 * 1024;
    private final MpscUnboundedArrayQueue<RangeCacheEntryWrapper> removalQueue = new MpscUnboundedArrayQueue<>(
            REMOVAL_QUEUE_CHUNK_SIZE);

    public Pair<Integer, Long> evictLEntriesBeforeTimestamp(long timestampNanos) {
        return evictEntries(
                (e, c) -> e.timestampNanos < timestampNanos ? EvictionResult.REMOVE : EvictionResult.STOP);
    }

    public Pair<Integer, Long> evictLeastAccessedEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        return evictEntries(
                (e, c) -> {
                    // stop eviction if we have already removed enough entries
                    if (c.removedSize >= sizeToFree) {
                        return EvictionResult.STOP;
                    }
                    return EvictionResult.REMOVE;
                });
    }

    public boolean addEntry(RangeCacheEntryWrapper newWrapper) {
        return removalQueue.offer(newWrapper);
    }

    enum EvictionResult {
        REMOVE, MISSING, STOP;

        boolean isContinueEviction() {
            return this != STOP;
        }

        boolean shouldRemoveFromQueue() {
            return this == REMOVE || this == MISSING;
        }
    }

    interface EvictionPredicate {
        EvictionResult test(RangeCacheEntryWrapper entry, RangeCacheRemovalCounters counters);
    }

    /**
     * Evict entries from the removal queue based on the provided eviction predicate.
     * This method is synchronized to prevent multiple threads from removing entries simultaneously.
     * An MPSC (Multiple Producer Single Consumer) queue is used as the removal queue, which expects a single consumer.
     *
     * @param evictionPredicate the predicate to determine if an entry should be evicted
     * @return the number of entries and the total size removed from the cache
     */
    private synchronized Pair<Integer, Long> evictEntries(EvictionPredicate evictionPredicate) {
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        handleQueue(evictionPredicate, counters);
        return handleRemovalResult(counters);
    }

    private void handleQueue(EvictionPredicate evictionPredicate,
                                                   RangeCacheRemovalCounters counters) {
        while (!Thread.currentThread().isInterrupted()) {
            RangeCacheEntryWrapper entry = removalQueue.peek();
            if (entry == null) {
                break;
            }
            EvictionResult evictionResult = handleEviction(evictionPredicate, entry, counters);
            if (evictionResult.shouldRemoveFromQueue()) {
                // remove the peeked entry from the queue
                removalQueue.poll();
                // recycle the entry after it has been removed from the queue
                entry.recycle();
            }
            if (!evictionResult.isContinueEviction()) {
                break;
            }
        }
    }

    private EvictionResult handleEviction(EvictionPredicate evictionPredicate, RangeCacheEntryWrapper entry,
                                          RangeCacheRemovalCounters counters) {
        EvictionResult evictionResult = entry.withWriteLock(e -> {
            EvictionResult result =
                    evaluateEvictionPredicate(evictionPredicate, counters, e);
            if (result == EvictionResult.REMOVE) {
                e.rangeCache.removeEntry(e.key, e.value, e, counters, true);
            }
            return result;
        });
        return evictionResult;
    }

    private static EvictionResult evaluateEvictionPredicate(EvictionPredicate evictionPredicate,
                                                    RangeCacheRemovalCounters counters, RangeCacheEntryWrapper entry) {
        if (entry.key == null) {
            // entry has been removed by another thread
            return EvictionResult.MISSING;
        }
        return evictionPredicate.test(entry, counters);
    }

    private Pair<Integer, Long> handleRemovalResult(RangeCacheRemovalCounters counters) {
        Pair<Integer, Long> result = Pair.of(counters.removedEntries, counters.removedSize);
        counters.recycle();
        return result;
    }
}

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
import com.google.common.annotations.VisibleForTesting;
import java.util.function.Consumer;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.jctools.queues.MpscUnboundedArrayQueue;

/**
 * A central queue to hold entries that are scheduled for removal from all range cache instances.
 * This class provides thread-safe eviction of cached entries based on age and memory pressure.
 *
 * This solution is influenced by S3FIFO algorithm (explanation at
 * https://blog.jasony.me/system/cache/2023/08/01/s3fifo and https://s3fifo.com/),
 * although it is not an implementation of S3FIFO. It accomplishes a similar goal as S3FIFO, but for the broker cache.
 * The goal of S3FIFO is to quickly remove entries that are unlikely to be accessed in the future. Similarly,
 * the PIP-430 broker cache removal queue aims to quickly and efficiently remove entries that would be unlikely to be
 * accessed in the future. The reason why S3FIFO is not implemented in broker cache is that in broker caching the
 * "expected read count" concept is used to determine whether an entry is likely to be accessed in the future. This
 * information is not available in generic cache implementations and in the broker cache this information can be used to
 * keep the entries in the cache longer that are likely to be accessed in the future.
 *
 * The S3FIFO algorithm explanation blog post states:
 * "While the eviction algorithm so far have been centered around LRU,
 * I believe modern eviction algorithms should be designed with FIFO queues."
 * This is one of the reasons why the PIP-430 broker cache removal queue is also based on a FIFO queue.
 *
 * Key features:
 * - Single threaded removal to avoid contention via MPSC (Multiple Producer Single Consumer) queue
 * - Entries are approximately ordered by timestamp for efficient eviction
 * - Two main eviction modes:
 * 1. Time-based: Removes entries older than specified timestamp
 * 2. Memory-based: Frees up memory by removing oldest/least accessed entries first
 */
class RangeCacheRemovalQueue {
    // The removal queue is unbounded, but we allocate memory in chunks to avoid frequent memory allocations.
    private static final int REMOVAL_QUEUE_CHUNK_SIZE = 128 * 1024;
    private final MpscUnboundedArrayQueue<RangeCacheEntryWrapper> removalQueue = new MpscUnboundedArrayQueue<>(
            REMOVAL_QUEUE_CHUNK_SIZE);
    private int maxRequeueCountWhenHasExpectedReads;
    private boolean extendTTLOfRecentlyAccessed;

    RangeCacheRemovalQueue(int maxRequeueCountWhenHasExpectedReads, boolean extendTTLOfRecentlyAccessed) {
        this.maxRequeueCountWhenHasExpectedReads = maxRequeueCountWhenHasExpectedReads;
        this.extendTTLOfRecentlyAccessed = extendTTLOfRecentlyAccessed;
    }

    public synchronized Pair<Integer, Long> evictLEntriesBeforeTimestamp(long timestampNanos) {
        return evictEntries((e, c) -> {
            boolean expired = e.timestampNanos < timestampNanos;
            if (expired) {
                if (shouldRequeue(e)) {
                    return EvictionResult.REQUEUE;
                } else  {
                    return EvictionResult.REMOVE;
                }
            } else {
                // if the entry is not expired, we stop eviction
                return EvictionResult.STOP;
            }
        });
    }

    public synchronized Pair<Integer, Long> evictLeastAccessedEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        return evictEntries((e, c) -> {
            // stop eviction if we have already removed enough entries
            if (c.removedSize >= sizeToFree) {
                return EvictionResult.STOP;
            }
            if (shouldRequeue(e)) {
                return EvictionResult.REQUEUE;
            }
            // remove by default since there's a need to free up more memory
            return EvictionResult.REMOVE;
        });
    }

    private boolean shouldRequeue(RangeCacheEntryWrapper e) {
        return  // If the entry is recently accessed and the feature is enabled, we requeue it.
                (extendTTLOfRecentlyAccessed && e.accessed)
                        // If the entry has expected reads, we requeue it if it has not exceeded the maximum requeue
                        // count.
                        || (e.value.hasExpectedReads() && e.requeueCount < maxRequeueCountWhenHasExpectedReads);
    }

    public boolean addEntry(RangeCacheEntryWrapper newWrapper) {
        return removalQueue.offer(newWrapper);
    }

    enum EvictionResult {
        REMOVE, REQUEUE, STOP, MISSING;

        boolean shouldStop() {
            return this == STOP;
        }

        boolean shouldRemovePeekedEntry() {
            return shouldRequeue() || shouldDrop();
        }

        boolean shouldRequeue() {
            return this == REQUEUE;
        }

        boolean shouldDrop() {
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

    private void handleQueue(EvictionPredicate evictionPredicate, RangeCacheRemovalCounters counters) {
        while (!Thread.currentThread().isInterrupted()) {
            RangeCacheEntryWrapper entry = removalQueue.peek();
            if (entry == null) {
                // no more entries to process
                break;
            }
            EvictionResult evictionResult = handleEviction(evictionPredicate, entry, counters);
            if (evictionResult.shouldRemovePeekedEntry()) {
                // remove the peeked entry from the queue
                removalQueue.poll();
            }
            if (evictionResult.shouldRequeue()) {
                // add the entry back to the end of the queue to be processed later
                removalQueue.add(entry);
            } else if (evictionResult.shouldDrop()) {
                // recycle the entry after it has been removed from the queue
                entry.recycle();
            }
            if (evictionResult.shouldStop()) {
                // stop eviction if the predicate returned STOP
                break;
            }
        }
    }

    private EvictionResult handleEviction(EvictionPredicate evictionPredicate, RangeCacheEntryWrapper entry,
                                          RangeCacheRemovalCounters counters) {
        EvictionResult evictionResult = entry.withWriteLock(e -> {
            EvictionResult result = evaluateEvictionPredicate(evictionPredicate, counters, e);
            if (result == EvictionResult.REMOVE) {
                e.rangeCache.removeEntry(e.key, e.value, e, counters, true);
            } else if (result == EvictionResult.REQUEUE) {
                e.markRequeued();
            }
            return result;
        });
        return evictionResult;
    }

    private static EvictionResult evaluateEvictionPredicate(EvictionPredicate evictionPredicate,
                                                            RangeCacheRemovalCounters counters,
                                                            RangeCacheEntryWrapper entry) {
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

    /**
     * Returns the actual size of the removal queue, excluding entries that don't have expected reads.
     * This method is used for testing purposes to verify actual size of retained cached entries.
     * This has a performance impact, so it should not be used in production code.
     * @return a pair containing the number of entries and their total size in bytes
     */
    @VisibleForTesting
    public synchronized Pair<Integer, Long> getNonEvictableSize() {
        final MutableInt entries = new MutableInt(0);
        final MutableLong bytesSize = new MutableLong(0L);
        forEachEntry((entryWrapper) -> {
            // only count entries that haven't met the expected reads condition
            // or don't have a read count handler
            if (entryWrapper.value.getReadCountHandler() == null || entryWrapper.value.hasExpectedReads()) {
                entries.increment();
                bytesSize.add(entryWrapper.size);
            }
        });
        return Pair.of(entries.getValue(), bytesSize.getValue());
    }

    /**
     * Iterates over all entries in the removal queue.
     * This is only for testing purposes and should not be used in production code
     * @param consumer the consumer to apply to each entry
     */
    @VisibleForTesting
    public synchronized void forEachEntry(Consumer<RangeCacheEntryWrapper> consumer) {
        RangeCacheEntryWrapper firstEntry = null;
        while (!Thread.currentThread().isInterrupted()) {
            RangeCacheEntryWrapper entry = removalQueue.peek();
            if (entry == null || entry == firstEntry) {
                // no more entries to process
                break;
            }
            boolean exists = entry.withWriteLock(wrapper -> {
                if (wrapper.key != null && wrapper.value != null) {
                    consumer.accept(wrapper);
                    return true;
                }
                return false;
            });
            // remove the entry from the queue
            removalQueue.poll();
            if (exists) {
                // add the entry to the end of the queue to retain the order
                removalQueue.add(entry);
                if (firstEntry == null) {
                    // remember the first entry to avoid infinite loop
                    firstEntry = entry;
                }
            }
        }
    }

    public synchronized void setMaxRequeueCountWhenHasExpectedReads(int maxRequeueCountWhenHasExpectedReads) {
        this.maxRequeueCountWhenHasExpectedReads = maxRequeueCountWhenHasExpectedReads;
    }

    public synchronized void setExtendTTLOfRecentlyAccessed(boolean extendTTLOfRecentlyAccessed) {
        this.extendTTLOfRecentlyAccessed = extendTTLOfRecentlyAccessed;
    }

    public boolean isEmpty() {
        return removalQueue.isEmpty();
    }
}

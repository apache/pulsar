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

import io.netty.util.IllegalReferenceCountException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Special type of cache where get() and delete() operations can be done over a range of keys.
 * The implementation avoids locks and synchronization by relying on ConcurrentSkipListMap for storing the entries.
 * Since there are no locks, it's necessary to ensure that a single entry in the cache is removed exactly once.
 * Removing an entry multiple times could result in the entries of the cache being released multiple times,
 * even while they are still in use. This is prevented by using a custom wrapper around the value to store in the map
 * that ensures that the value is removed from the map only if the exact same instance is present in the map.
 * There's also a check that ensures that the value matches the key. This is used to detect races without impacting
 * consistency.
 */
@Slf4j
class RangeCache {
    private final ConcurrentNavigableMap<Position, RangeCacheEntryWrapper> entries;
    private final RangeCacheRemovalQueue removalQueue;
    private AtomicLong size; // Total size of values stored in cache

    /**
     * Construct a new RangeCache.
     */
    public RangeCache(RangeCacheRemovalQueue removalQueue) {
        this.removalQueue = removalQueue;
        this.entries = new ConcurrentSkipListMap<>();
        this.size = new AtomicLong(0);
    }

    /**
     * Insert.
     *
     * @param key
     * @param value ref counted value with at least 1 ref to pass on the cache
     * @return whether the entry was inserted in the cache
     */
    public boolean put(Position key, CachedEntry value) {
        // retain value so that it's not released before we put it in the cache and calculate the weight
        value.retain();
        try {
            if (!value.matchesKey(key)) {
                throw new IllegalArgumentException("Value '" + value + "' does not match key '" + key + "'");
            }
            long entrySize = value.getLength();
            boolean added = RangeCacheEntryWrapper.withNewInstance(this, key, value, entrySize, newWrapper -> {
                if (removalQueue.addEntry(newWrapper) && entries.putIfAbsent(key, newWrapper) == null) {
                    this.size.addAndGet(entrySize);
                    return true;
                } else {
                    // recycle the new wrapper as it was not used
                    newWrapper.recycle();
                    return false;
                }
            });
            return added;
        } finally {
            value.release();
        }
    }

    public boolean exists(Position key) {
        return key != null ? entries.containsKey(key) : true;
    }

    /**
     * Get the value associated with the key and increment the reference count of it.
     * The caller is responsible for releasing the reference.
     */
    public CachedEntry get(Position key) {
        return getValueFromWrapper(key, entries.get(key));
    }

    private CachedEntry getValueFromWrapper(Position key, RangeCacheEntryWrapper valueWrapper) {
        if (valueWrapper == null) {
            return null;
        } else {
            CachedEntry value = valueWrapper.getValue(key);
            return getRetainedValueMatchingKey(key, value);
        }
    }

    /**
     * @apiNote the returned value must be released if it's not null
     */
    private CachedEntry getValueMatchingEntry(Map.Entry<Position, RangeCacheEntryWrapper> entry) {
        CachedEntry valueMatchingEntry = RangeCacheEntryWrapper.getValueMatchingMapEntry(entry);
        return getRetainedValueMatchingKey(entry.getKey(), valueMatchingEntry);
    }

    // validates that the value matches the key and that the value has not been recycled
    // which are possible due to the lack of exclusive locks in the cache and the use of reference counted objects
    /**
     * @apiNote the returned value must be released if it's not null
     */
    private CachedEntry getRetainedValueMatchingKey(Position key, CachedEntry value) {
        if (value == null) {
            // the wrapper has been recycled and contains another key
            return null;
        }
        try {
            value.retain();
        } catch (IllegalReferenceCountException e) {
            // Value was already deallocated
            return null;
        }
        // check that the value matches the key and that there's at least 2 references to it since
        // the cache should be holding one reference and a new reference was just added in this method
        if (value.refCnt() > 1 && value.matchesKey(key)) {
            return value;
        } else {
            // Value or IdentityWrapper was recycled and already contains another value
            // release the reference added in this method
            value.release();
            return null;
        }
    }

    /**
     *
     * @param first
     *            the first key in the range
     * @param last
     *            the last key in the range (inclusive)
     * @return a collections of the value found in cache
     */
    public Collection<CachedEntry> getRange(Position first, Position last) {
        List<CachedEntry> values = new ArrayList();

        // Return the values of the entries found in cache
        for (Map.Entry<Position, RangeCacheEntryWrapper> entry : entries.subMap(first, true, last, true)
                .entrySet()) {
            CachedEntry value = getValueMatchingEntry(entry);
            if (value != null) {
                values.add(value);
            }
        }

        return values;
    }

    /**
     *
     * @param first
     * @param last
     * @param lastInclusive
     * @return an pair of ints, containing the number of removed entries and the total size
     */
    public Pair<Integer, Long> removeRange(Position first, Position last, boolean lastInclusive) {
        if (log.isDebugEnabled()) {
            log.debug("Removing entries in range [{}, {}], lastInclusive: {}", first, last, lastInclusive);
        }
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        Map<Position, RangeCacheEntryWrapper> subMap = entries.subMap(first, true, last, lastInclusive);
        for (Map.Entry<Position, RangeCacheEntryWrapper> entry : subMap.entrySet()) {
            removeEntryWithWriteLock(entry.getKey(), entry.getValue(), counters);
        }
        return handleRemovalResult(counters);
    }

    boolean removeEntryWithWriteLock(Position expectedKey, RangeCacheEntryWrapper entryWrapper,
                                     RangeCacheRemovalCounters counters) {
        return entryWrapper.withWriteLock(e -> {
            if (e.key == null || e.key != expectedKey) {
                // entry has already been removed
                return false;
            }
            return removeEntry(e.key, e.value, e, counters, false);
        });
    }

    /**
     * Remove the entry from the cache. This must be called within a function passed to
     * {@link RangeCacheEntryWrapper#withWriteLock(Function)}.
     * @param key the expected key of the entry
     * @param value the expected value of the entry
     * @param entryWrapper the entry wrapper instance
     * @param counters the removal counters
     * @return true if the entry was removed, false otherwise
     */
    boolean removeEntry(Position key, CachedEntry value, RangeCacheEntryWrapper entryWrapper,
                        RangeCacheRemovalCounters counters, boolean updateSize) {
        // always remove the entry from the map
        entries.remove(key, entryWrapper);
        if (value == null) {
            // the wrapper has already been recycled and contains another key
            return false;
        }
        try {
            // add extra retain to avoid value being released while we are removing it
            value.retain();
        } catch (IllegalReferenceCountException e) {
            return false;
        }
        try {
            if (!value.matchesKey(key)) {
                return false;
            }
            long removedSize = entryWrapper.markRemoved(key, value);
            if (removedSize > -1) {
                counters.entryRemoved(removedSize);
                if (updateSize) {
                    size.addAndGet(-removedSize);
                }
                if (value.refCnt() > 1) {
                    // remove the cache reference
                    value.release();
                } else {
                    log.info("Unexpected refCnt {} for key {}, removed entry without releasing the value",
                            value.refCnt(), key);
                }
                return true;
            } else {
                return false;
            }
        } finally {
            // remove the extra retain
            value.release();
        }
    }

    private Pair<Integer, Long> handleRemovalResult(RangeCacheRemovalCounters counters) {
        size.addAndGet(-counters.removedSize);
        Pair<Integer, Long> result = Pair.of(counters.removedEntries, counters.removedSize);
        counters.recycle();
        return result;
    }

    /**
     * Just for testing. Getting the number of entries is very expensive on the concurrent map
     */
    protected long getNumberOfEntries() {
        return entries.size();
    }

    public long getSize() {
        return size.get();
    }

    /**
     * Remove all the entries from the cache.
     *
     * @return size of removed entries
     */
    public Pair<Integer, Long> clear() {
        if (log.isDebugEnabled()) {
            log.debug("Clearing the cache with {} entries and size {}", entries.size(), size.get());
        }
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        while (!Thread.currentThread().isInterrupted()) {
            Map.Entry<Position, RangeCacheEntryWrapper> entry = entries.firstEntry();
            if (entry == null) {
                break;
            }
            removeEntryWithWriteLock(entry.getKey(), entry.getValue(), counters);
        }
        return handleRemovalResult(counters);
    }
}

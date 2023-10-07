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
package org.apache.bookkeeper.mledger.util;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.Lists;
import io.netty.util.ReferenceCounted;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Special type of cache where get() and delete() operations can be done over a range of keys.
 *
 * @param <Key>
 *            Cache key. Needs to be Comparable
 * @param <Value>
 *            Cache value
 */
public class RangeCache<Key extends Comparable<Key>, Value extends ReferenceCounted> {
    // Map from key to nodes inside the linked list
    private final ConcurrentNavigableMap<Key, Value> entries;
    private AtomicLong size; // Total size of values stored in cache
    private final Weighter<Value> weighter; // Weighter object used to extract the size from values
    private final TimestampExtractor<Value> timestampExtractor; // Extract the timestamp associated with a value

    /**
     * Construct a new RangeLruCache with default Weighter.
     */
    public RangeCache() {
        this(new DefaultWeighter<>(), (x) -> System.nanoTime());
    }

    /**
     * Construct a new RangeLruCache.
     *
     * @param weighter
     *            a custom weighter to compute the size of each stored value
     */
    public RangeCache(Weighter<Value> weighter, TimestampExtractor<Value> timestampExtractor) {
        this.size = new AtomicLong(0);
        this.entries = new ConcurrentSkipListMap<>();
        this.weighter = weighter;
        this.timestampExtractor = timestampExtractor;
    }

    /**
     * Insert.
     *
     * @param key
     * @param value
     *            ref counted value with at least 1 ref to pass on the cache
     * @return whether the entry was inserted in the cache
     */
    public boolean put(Key key, Value value) {
        // retain value so that it's not released before we put it in the cache and calculate the weight
        value.retain();
        try {
            if (entries.putIfAbsent(key, value) == null) {
                size.addAndGet(weighter.getSize(value));
                return true;
            } else {
                return false;
            }
        } finally {
            value.release();
        }
    }

    public Value get(Key key) {
        Value value = entries.get(key);
        if (value == null) {
            return null;
        } else {
            try {
                value.retain();
                return value;
            } catch (Throwable t) {
                // Value was already destroyed between get() and retain()
                return null;
            }
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
    public Collection<Value> getRange(Key first, Key last) {
        List<Value> values = Lists.newArrayList();

        // Return the values of the entries found in cache
        for (Value value : entries.subMap(first, true, last, true).values()) {
            try {
                value.retain();
                values.add(value);
            } catch (Throwable t) {
                // Value was already destroyed between get() and retain()
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
    public Pair<Integer, Long> removeRange(Key first, Key last, boolean lastInclusive) {
        Map<Key, Value> subMap = entries.subMap(first, true, last, lastInclusive);

        int removedEntries = 0;
        long removedSize = 0;

        for (Key key : subMap.keySet()) {
            Value value = entries.remove(key);
            if (value == null) {
                continue;
            }

            removedSize += weighter.getSize(value);
            value.release();
            ++removedEntries;
        }

        size.addAndGet(-removedSize);

        return Pair.of(removedEntries, removedSize);
    }

    /**
     *
     * @param minSize
     * @return a pair containing the number of entries evicted and their total size
     */
    public Pair<Integer, Long> evictLeastAccessedEntries(long minSize) {
        checkArgument(minSize > 0);

        long removedSize = 0;
        int removedEntries = 0;

        while (removedSize < minSize) {
            Map.Entry<Key, Value> entry = entries.pollFirstEntry();
            if (entry == null) {
                break;
            }

            Value value = entry.getValue();
            ++removedEntries;
            removedSize += weighter.getSize(value);
            value.release();
        }

        size.addAndGet(-removedSize);
        return Pair.of(removedEntries, removedSize);
    }

    /**
    *
    * @param maxTimestamp the max timestamp of the entries to be evicted
    * @return the tota
    */
   public Pair<Integer, Long> evictLEntriesBeforeTimestamp(long maxTimestamp) {
       long removedSize = 0;
       int removedCount = 0;

       while (true) {
           Map.Entry<Key, Value> entry = entries.firstEntry();
           if (entry == null || timestampExtractor.getTimestamp(entry.getValue()) > maxTimestamp) {
               break;
           }
           Value value = entry.getValue();
           boolean removeHits = entries.remove(entry.getKey(), value);
           if (!removeHits) {
               break;
           }

           removedSize += weighter.getSize(value);
           removedCount++;
           value.release();
       }

       size.addAndGet(-removedSize);
       return Pair.of(removedCount, removedSize);
   }

    /**
     * Just for testing. Getting the number of entries is very expensive on the conncurrent map
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
    public synchronized Pair<Integer, Long> clear() {
        long removedSize = 0;
        int removedCount = 0;

        while (true) {
            Map.Entry<Key, Value> entry = entries.pollFirstEntry();
            if (entry == null) {
                break;
            }
            Value value = entry.getValue();
            removedSize += weighter.getSize(value);
            removedCount++;
            value.release();
        }

        size.getAndAdd(-removedSize);
        return Pair.of(removedCount, removedSize);
    }

    /**
     * Interface of a object that is able to the extract the "weight" (size/cost/space) of the cached values.
     *
     * @param <ValueT>
     */
    public interface Weighter<ValueT> {
        long getSize(ValueT value);
    }

    /**
     * Interface of a object that is able to the extract the "timestamp" of the cached values.
     *
     * @param <ValueT>
     */
    public interface TimestampExtractor<ValueT> {
        long getTimestamp(ValueT value);
    }

    /**
     * Default cache weighter, every value is assumed the same cost.
     *
     * @param <Value>
     */
    private static class DefaultWeighter<Value> implements Weighter<Value> {
        @Override
        public long getSize(Value value) {
            return 1;
        }
    }

}

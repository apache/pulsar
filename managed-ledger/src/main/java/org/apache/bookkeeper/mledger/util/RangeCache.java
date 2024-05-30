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
package org.apache.bookkeeper.mledger.util;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Predicate;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCounted;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.util.RangeCache.ValueWithKeyValidation;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Special type of cache where get() and delete() operations can be done over a range of keys.
 *
 * @param <Key>
 *            Cache key. Needs to be Comparable
 * @param <Value>
 *            Cache value
 */
public class RangeCache<Key extends Comparable<Key>, Value extends ValueWithKeyValidation<Key>> {
    public interface ValueWithKeyValidation<T> extends ReferenceCounted {
        boolean matchesKey(T key);
    }

    // Map from key to nodes inside the linked list
    private final ConcurrentNavigableMap<Key, IdentityWrapper<Value>> entries;
    private AtomicLong size; // Total size of values stored in cache
    private final Weighter<Value> weighter; // Weighter object used to extract the size from values
    private final TimestampExtractor<Value> timestampExtractor; // Extract the timestamp associated with a value

    /**
     * Wrapper around the value to store in Map. This is needed to ensure that a specific instance can be removed from
     * the map by calling the {@link Map#remove(Object, Object)} method. Certain race conditions could result in the
     * wrong value being removed from the map. The instances of this class are recycled to avoid creating new objects.
     */
    private static class IdentityWrapper<T> {
        private final Handle<IdentityWrapper> recyclerHandle;
        private static final Recycler<IdentityWrapper> RECYCLER = new Recycler<IdentityWrapper>() {
            @Override
            protected IdentityWrapper newObject(Handle<IdentityWrapper> recyclerHandle) {
                return new IdentityWrapper(recyclerHandle);
            }
        };
        private T value;

        private IdentityWrapper(Handle<IdentityWrapper> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static <T> IdentityWrapper<T> create(T value) {
            IdentityWrapper<T> identityWrapper = RECYCLER.get();
            identityWrapper.value = value;
            return identityWrapper;
        }

        T get() {
            return value;
        }

        void recycle() {
            value = null;
            recyclerHandle.recycle(this);
        }

        @Override
        public boolean equals(Object o) {
            // only match exact identity of the value
            return this == o;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(value);
        }
    }

    /**
     * Mutable object to store the number of entries and the total size removed from the cache. The instances
     * are recycled to avoid creating new instances.
     */
    private static class RemovalCounters {
        private final Handle<RemovalCounters> recyclerHandle;
        private static final Recycler<RemovalCounters> RECYCLER = new Recycler<RemovalCounters>() {
            @Override
            protected RemovalCounters newObject(Handle<RemovalCounters> recyclerHandle) {
                return new RemovalCounters(recyclerHandle);
            }
        };
        int removedEntries;
        long removedSize;
        private RemovalCounters(Handle<RemovalCounters> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static <T> RemovalCounters create() {
            RemovalCounters results = RECYCLER.get();
            results.removedEntries = 0;
            results.removedSize = 0;
            return results;
        }

        void recycle() {
            removedEntries = 0;
            removedSize = 0;
            recyclerHandle.recycle(this);
        }

        public void entryRemoved(long size) {
            removedSize += size;
            removedEntries++;
        }
    }

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
     * @param value ref counted value with at least 1 ref to pass on the cache
     * @return whether the entry was inserted in the cache
     */
    public boolean put(Key key, Value value) {
        // retain value so that it's not released before we put it in the cache and calculate the weight
        value.retain();
        try {
            if (!value.matchesKey(key)) {
                throw new IllegalArgumentException("Value '" + value + "' does not match key '" + key + "'");
            }
            IdentityWrapper<Value> newWrapper = IdentityWrapper.create(value);
            if (entries.putIfAbsent(key, newWrapper) == null) {
                size.addAndGet(weighter.getSize(value));
                return true;
            } else {
                // recycle the new wrapper as it was not used
                newWrapper.recycle();
                return false;
            }
        } finally {
            value.release();
        }
    }

    public boolean exists(Key key) {
        return key != null ? entries.containsKey(key) : true;
    }

    public Value get(Key key) {
        return getValue(key, entries.get(key));
    }

    private  Value getValue(Key key, IdentityWrapper<Value> valueWrapper) {
        if (valueWrapper == null) {
            return null;
        } else {
            Value value = valueWrapper.get();
            try {
                value.retain();
            } catch (IllegalReferenceCountException e) {
                // Value was already deallocated
                return null;
            }
            if (value.matchesKey(key)) {
                return value;
            } else {
                // Value or IdentityWrapper was recycled and already contains another value
                value.release();
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
        List<Value> values = new ArrayList();

        // Return the values of the entries found in cache
        for (Map.Entry<Key, IdentityWrapper<Value>> entry : entries.subMap(first, true, last, true).entrySet()) {
            Value value = getValue(entry.getKey(), entry.getValue());
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
    public Pair<Integer, Long> removeRange(Key first, Key last, boolean lastInclusive) {
        RemovalCounters counters = RemovalCounters.create();
        Map<Key, IdentityWrapper<Value>> subMap = entries.subMap(first, true, last, lastInclusive);
        for (Map.Entry<Key, IdentityWrapper<Value>> entry : subMap.entrySet()) {
            removeEntry(entry, counters);
        }
        return handleRemovalResult(counters);
    }

    enum RemoveEntryResult {
        ENTRY_REMOVED,
        CONTINUE_LOOP,
        BREAK_LOOP;
    }

    private RemoveEntryResult removeEntry(Map.Entry<Key, IdentityWrapper<Value>> entry, RemovalCounters counters) {
        return removeEntry(entry, counters, (x) -> true);
    }

    private RemoveEntryResult removeEntry(Map.Entry<Key, IdentityWrapper<Value>> entry, RemovalCounters counters,
                                          Predicate<Value> removeCondition) {
        Key key = entry.getKey();
        IdentityWrapper<Value> identityWrapper = entry.getValue();
        Value value = identityWrapper.get();
        try {
            // add extra retain to avoid value being released while we are removing it
            value.retain();
        } catch (IllegalReferenceCountException e) {
            // Value was already released
            return RemoveEntryResult.CONTINUE_LOOP;
        }
        try {
            if (!removeCondition.test(value)) {
                return RemoveEntryResult.BREAK_LOOP;
            }
            // check that the value hasn't been recycled in between
            if (value.matchesKey(key) && entries.remove(key, identityWrapper)) {
                identityWrapper.recycle();
                counters.entryRemoved(weighter.getSize(value));
                // remove the cache reference
                value.release();
            }
        } finally {
            // remove the extra retain
            value.release();
        }
        return RemoveEntryResult.ENTRY_REMOVED;
    }

    private Pair<Integer, Long> handleRemovalResult(RemovalCounters counters) {
        size.addAndGet(-counters.removedSize);
        Pair<Integer, Long> result = Pair.of(counters.removedEntries, counters.removedSize);
        counters.recycle();
        return result;
    }

    /**
     *
     * @param minSize
     * @return a pair containing the number of entries evicted and their total size
     */
    public Pair<Integer, Long> evictLeastAccessedEntries(long minSize) {
        checkArgument(minSize > 0);
        RemovalCounters counters = RemovalCounters.create();
        while (counters.removedSize < minSize) {
            Map.Entry<Key, IdentityWrapper<Value>> entry = entries.firstEntry();
            if (entry == null) {
                break;
            }
            removeEntry(entry, counters);
        }
        return handleRemovalResult(counters);
    }

    /**
    *
    * @param maxTimestamp the max timestamp of the entries to be evicted
    * @return the tota
    */
   public Pair<Integer, Long> evictLEntriesBeforeTimestamp(long maxTimestamp) {
       RemovalCounters counters = RemovalCounters.create();
       while (true) {
           Map.Entry<Key, IdentityWrapper<Value>> entry = entries.firstEntry();
           if (entry == null) {
               break;
           }
           if (removeEntry(entry, counters, value -> timestampExtractor.getTimestamp(value) <= maxTimestamp)
                   == RemoveEntryResult.BREAK_LOOP) {
               break;
           }
       }
       return handleRemovalResult(counters);
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
        RemovalCounters counters = RemovalCounters.create();
        while (true) {
            Map.Entry<Key, IdentityWrapper<Value>> entry = entries.firstEntry();
            if (entry == null) {
                break;
            }
            removeEntry(entry, counters);
        }
        return handleRemovalResult(counters);
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

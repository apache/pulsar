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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.util.RangeCache.ValueWithKeyValidation;
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
 *
 * @param <Key>
 *            Cache key. Needs to be Comparable
 * @param <Value>
 *            Cache value
 */
@Slf4j
public class RangeCache<Key extends Comparable<Key>, Value extends ValueWithKeyValidation<Key>> {
    public interface ValueWithKeyValidation<T> extends ReferenceCounted {
        boolean matchesKey(T key);
    }

    // Map from key to nodes inside the linked list
    private final ConcurrentNavigableMap<Key, EntryWrapper<Key, Value>> entries;
    private AtomicLong size; // Total size of values stored in cache
    private final Weighter<Value> weighter; // Weighter object used to extract the size from values
    private final TimestampExtractor<Value> timestampExtractor; // Extract the timestamp associated with a value

    /**
     * Wrapper around the value to store in Map. This is needed to ensure that a specific instance can be removed from
     * the map by calling the {@link Map#remove(Object, Object)} method. Certain race conditions could result in the
     * wrong value being removed from the map. The instances of this class are recycled to avoid creating new objects.
     */
    private static class EntryWrapper<K, V> {
        private final Handle<EntryWrapper> recyclerHandle;
        private static final Recycler<EntryWrapper> RECYCLER = new Recycler<EntryWrapper>() {
            @Override
            protected EntryWrapper newObject(Handle<EntryWrapper> recyclerHandle) {
                return new EntryWrapper(recyclerHandle);
            }
        };
        private final StampedLock lock = new StampedLock();
        private K key;
        private V value;
        long size;

        private EntryWrapper(Handle<EntryWrapper> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static <K, V> EntryWrapper<K, V> create(K key, V value, long size) {
            EntryWrapper<K, V> entryWrapper = RECYCLER.get();
            long stamp = entryWrapper.lock.writeLock();
            entryWrapper.key = key;
            entryWrapper.value = value;
            entryWrapper.size = size;
            entryWrapper.lock.unlockWrite(stamp);
            return entryWrapper;
        }

        K getKey() {
            long stamp = lock.tryOptimisticRead();
            K localKey = key;
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                localKey = key;
                lock.unlockRead(stamp);
            }
            return localKey;
        }

        V getValue(K key) {
            long stamp = lock.tryOptimisticRead();
            K localKey = this.key;
            V localValue = this.value;
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                localKey = this.key;
                localValue = this.value;
                lock.unlockRead(stamp);
            }
            if (localKey != key) {
                return null;
            }
            return localValue;
        }

        long getSize() {
            long stamp = lock.tryOptimisticRead();
            long localSize = size;
            if (!lock.validate(stamp)) {
                stamp = lock.readLock();
                localSize = size;
                lock.unlockRead(stamp);
            }
            return localSize;
        }

        void recycle() {
            key = null;
            value = null;
            size = 0;
            recyclerHandle.recycle(this);
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
            long entrySize = weighter.getSize(value);
            EntryWrapper<Key, Value> newWrapper = EntryWrapper.create(key, value, entrySize);
            if (entries.putIfAbsent(key, newWrapper) == null) {
                this.size.addAndGet(entrySize);
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

    /**
     * Get the value associated with the key and increment the reference count of it.
     * The caller is responsible for releasing the reference.
     */
    public Value get(Key key) {
        return getValue(key, entries.get(key));
    }

    private  Value getValue(Key key, EntryWrapper<Key, Value> valueWrapper) {
        if (valueWrapper == null) {
            return null;
        } else {
            Value value = valueWrapper.getValue(key);
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
        for (Map.Entry<Key, EntryWrapper<Key, Value>> entry : entries.subMap(first, true, last, true).entrySet()) {
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
        Map<Key, EntryWrapper<Key, Value>> subMap = entries.subMap(first, true, last, lastInclusive);
        for (Map.Entry<Key, EntryWrapper<Key, Value>> entry : subMap.entrySet()) {
            removeEntry(entry, counters, true);
        }
        return handleRemovalResult(counters);
    }

    enum RemoveEntryResult {
        ENTRY_REMOVED,
        CONTINUE_LOOP,
        BREAK_LOOP;
    }

    private RemoveEntryResult removeEntry(Map.Entry<Key, EntryWrapper<Key, Value>> entry, RemovalCounters counters,
                                          boolean skipInvalid) {
        return removeEntry(entry, counters, skipInvalid, x -> true);
    }

    private RemoveEntryResult removeEntry(Map.Entry<Key, EntryWrapper<Key, Value>> entry, RemovalCounters counters,
                                          boolean skipInvalid, Predicate<Value> removeCondition) {
        Key key = entry.getKey();
        EntryWrapper<Key, Value> entryWrapper = entry.getValue();
        Value value = entryWrapper.getValue(key);
        if (value == null) {
            // the wrapper has already been recycled and contains another key
            if (!skipInvalid) {
                EntryWrapper<Key, Value> removed = entries.remove(key);
                if (removed != null) {
                    // log and remove the entry without releasing the value
                    log.info("Key {} does not match the entry's value wrapper's key {}, removed entry by key without "
                            + "releasing the value", key, entryWrapper.getKey());
                    counters.entryRemoved(removed.getSize());
                    return RemoveEntryResult.ENTRY_REMOVED;
                }
            }
            return RemoveEntryResult.CONTINUE_LOOP;
        }
        try {
            // add extra retain to avoid value being released while we are removing it
            value.retain();
        } catch (IllegalReferenceCountException e) {
            // Value was already released
            if (!skipInvalid) {
                // remove the specific entry without releasing the value
                if (entries.remove(key, entryWrapper)) {
                    log.info("Value was already released for key {}, removed entry without releasing the value", key);
                    counters.entryRemoved(entryWrapper.getSize());
                    return RemoveEntryResult.ENTRY_REMOVED;
                }
            }
            return RemoveEntryResult.CONTINUE_LOOP;
        }
        if (!value.matchesKey(key)) {
            // this is unexpected since the IdentityWrapper.getValue(key) already checked that the value matches the key
            log.warn("Unexpected race condition. Value {} does not match the key {}. Removing entry.", value, key);
        }
        try {
            if (!removeCondition.test(value)) {
                return RemoveEntryResult.BREAK_LOOP;
            }
            if (!skipInvalid) {
                // remove the specific entry
                boolean entryRemoved = entries.remove(key, entryWrapper);
                if (entryRemoved) {
                    counters.entryRemoved(entryWrapper.getSize());
                    // check that the value hasn't been recycled in between
                    // there should be at least 2 references since this method adds one and the cache should have
                    // one reference. it is valid that the value contains references even after the key has been
                    // removed from the cache
                    if (value.refCnt() > 1) {
                        entryWrapper.recycle();
                        // remove the cache reference
                        value.release();
                    } else {
                        log.info("Unexpected refCnt {} for key {}, removed entry without releasing the value",
                                value.refCnt(), key);
                    }
                }
            } else if (skipInvalid && value.refCnt() > 1 && entries.remove(key, entryWrapper)) {
                // when skipInvalid is true, we don't remove the entry if it doesn't match matches the key
                // or the refCnt is invalid
                counters.entryRemoved(entryWrapper.getSize());
                entryWrapper.recycle();
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
        while (counters.removedSize < minSize && !Thread.currentThread().isInterrupted()) {
            Map.Entry<Key, EntryWrapper<Key, Value>> entry = entries.firstEntry();
            if (entry == null) {
                break;
            }
            removeEntry(entry, counters, false);
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
       while (!Thread.currentThread().isInterrupted()) {
           Map.Entry<Key, EntryWrapper<Key, Value>> entry = entries.firstEntry();
           if (entry == null) {
               break;
           }
           if (removeEntry(entry, counters, false, value -> timestampExtractor.getTimestamp(value) <= maxTimestamp)
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
    public Pair<Integer, Long> clear() {
        RemovalCounters counters = RemovalCounters.create();
        while (!Thread.currentThread().isInterrupted()) {
            Map.Entry<Key, EntryWrapper<Key, Value>> entry = entries.firstEntry();
            if (entry == null) {
                break;
            }
            removeEntry(entry, counters, false);
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

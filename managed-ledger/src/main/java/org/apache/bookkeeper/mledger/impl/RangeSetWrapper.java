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
package org.apache.bookkeeper.mledger.impl;


import com.google.common.collect.Range;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class RangeSetWrapper<T extends Comparable<T>> implements LongPairRangeSet<T>, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RangeSetWrapper.class);

    private final LongPairRangeSet<T> rangeSet;
    private final ManagedCursorImpl managedCursor;
    private final LongPairConsumer<T> rangeConverter;
    private final ManagedLedgerConfig config;
    private final ScheduledFuture<?> future;
    private final OrderedScheduler scheduler;
    private final LruCache<Long, Byte> lruCounter = new LruCache<>();
    private final Set<Long> dirtyKeyRecorder = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public RangeSetWrapper(LongPairConsumer<T> rangeConverter, ManagedCursorImpl managedCursor) {
        checkNotNull(managedCursor);
        this.config = managedCursor.getConfig();
        this.managedCursor = managedCursor;
        this.rangeConverter = rangeConverter;
        this.rangeSet = config.isUnackedRangesOpenCacheSetEnabled()
                ? new ConcurrentOpenLongPairRangeSet<>(4096, rangeConverter)
                : new LongPairRangeSet.DefaultRangeSet<>(rangeConverter);
        if (managedCursor.getManagedLedger() instanceof ManagedLedgerImpl && config.isEnableLruCacheMaxUnackedRanges()) {
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) managedCursor.getManagedLedger();
            scheduler = managedLedger.getScheduledExecutor();
            future = scheduler.scheduleWithFixedDelay(
                    new LruTask(), 1, 5, TimeUnit.SECONDS);
        } else {
            future = null;
            scheduler = null;
        }
    }

    @Override
    public void addOpenClosed(long lowerKey, long lowerValue, long upperKey, long upperValue) {
        lruTouch(upperKey);
        dirtyKeyRecorder.add(upperKey);
        rangeSet.addOpenClosed(lowerKey, lowerValue, upperKey, upperValue);
    }

    private void lruTouch(long ledgerId) {
        if (config.isEnableLruCacheMaxUnackedRanges()) {
            scheduler.execute(() -> lruCounter.touch(ledgerId));
        }
    }

    public boolean isReachLruSwitchThreshold() {
        if (rangeSet instanceof ConcurrentOpenLongPairRangeSet) {
            ConcurrentOpenLongPairRangeSet<T> set = (ConcurrentOpenLongPairRangeSet<T>) rangeSet;
            return set.getRangeBitSetMapMemorySize() > config.getMaxUnackedRangesInMemoryBytes();
        }
        // every position has 3 long properties
        return (long) size() * 24 > config.getMaxUnackedRangesInMemoryBytes();
    }

    public LongPairRangeSet<T> getRangeSet() {
        return rangeSet;
    }

    public LruCache<Long, Byte> getLruCounter() {
        return lruCounter;
    }

    @Override
    public boolean contains(long key, long value) {
        lruTouch(key);
        boolean isContain = rangeSet.contains(key, value);
        if (!isContain && shouldLoadLruRangeFromLedger(key, value)) {
            tryLoadLruRangeFromLedger(key);
            return rangeSet.contains(key, value);
        }
        return isContain;
    }

    @Override
    public Range<T> rangeContaining(long key, long value) {
        lruTouch(key);
        Range<T> range = rangeSet.rangeContaining(key, value);
        if(range == null && shouldLoadLruRangeFromLedger(key, value)){
            tryLoadLruRangeFromLedger(key);
            return rangeSet.rangeContaining(key, value);
        }
        return range;
    }

    @Override
    public void removeAtMost(long key, long value) {
        lruTouch(key);
        rangeSet.removeAtMost(key, value);
    }

    public boolean shouldLoadLruRangeFromLedger(long key, long value) {
        if (!config.isEnableLruCacheMaxUnackedRanges()) {
            return false;
        }
        T range = rangeConverter.apply(key, value);
        Range<T> lower = rangeSet.firstRange();
        Range<T> upper = rangeSet.lastRange();
        if (lower == null || upper == null) {
            return managedCursor.getRangeMarker().containsKey(key);
        }
        if (range.compareTo(lower.lowerEndpoint()) >= 0 || range.compareTo(upper.upperEndpoint()) <= 0) {
            return false;
        }
        return true;
    }

    private void tryLoadLruRangeFromLedger(long key) {
        if (config.isEnableLruCacheMaxUnackedRanges()) {
            MLDataFormats.NestedPositionInfo positionInfo = managedCursor.getRangeMarker().get(key);
            if (positionInfo == null) {
                return;
            }
            try {
                Enumeration<LedgerEntry> entryEnumeration = managedCursor.getCursorLedgerHandle()
                                .readEntries(positionInfo.getEntryId(), positionInfo.getEntryId());
                if(entryEnumeration.hasMoreElements()){
                    LedgerEntry ledgerEntry = entryEnumeration.nextElement();
                    MLDataFormats.PositionInfo entryPosition =
                            MLDataFormats.PositionInfo.parseFrom(ledgerEntry.getEntry());
                    managedCursor.recoverIndividualDeletedMessages(entryPosition.getIndividualDeletedMessagesList(),
                            false);
                }
            } catch (Exception e) {
                log.error("load lru entry failed", e);
            }
        }
    }

    public void add(Range<LongPair> range) {
        if (!(rangeSet instanceof ConcurrentOpenLongPairRangeSet)) {
            throw new UnsupportedOperationException("Only ConcurrentOpenLongPairRangeSet support this method");
        }
        ConcurrentOpenLongPairRangeSet<LongPair> set = (ConcurrentOpenLongPairRangeSet<LongPair>) rangeSet;
        set.add(range);
    }

    public void remove(Range<T> range) {
        if (rangeSet instanceof ConcurrentOpenLongPairRangeSet) {
            ConcurrentOpenLongPairRangeSet<T> set = (ConcurrentOpenLongPairRangeSet<T>) rangeSet;
            Range<LongPair> longPairRange = (Range<LongPair>) range;
            set.remove(longPairRange);
        } else {
            LongPairRangeSet.DefaultRangeSet<T> set = (LongPairRangeSet.DefaultRangeSet<T>) rangeSet;
            set.remove(range);
        }
    }

    @Override
    public boolean isEmpty() {
        return rangeSet.isEmpty();
    }

    @Override
    public void clear() {
        rangeSet.clear();
    }

    @Override
    public Range<T> span() {
        return rangeSet.span();
    }

    @Override
    public List<Range<T>> asRanges() {
        Collection<Range<T>> collection = rangeSet.asRanges();
        if (collection instanceof List) {
            return (List<Range<T>>) collection;
        }
        return new ArrayList<>(collection);
    }

    @Override
    public void forEach(RangeProcessor<T> action) {
        rangeSet.forEach(action);
    }

    @Override
    public void forEach(RangeProcessor<T> action, LongPairConsumer<? extends T> consumer) {
        rangeSet.forEach(action, consumer);
    }

    @Override
    public int size() {
        return rangeSet.size();
    }

    @Override
    public Range<T> firstRange() {
        return rangeSet.firstRange();
    }

    @Override
    public Range<T> lastRange() {
        return rangeSet.lastRange();
    }

    @Override
    public void close() throws Exception {
        if (future != null && !future.isCancelled()) {
            future.cancel(true);
        }
    }

    public Set<Long> getDirtyKeyRecords() {
        Set<Long> set = new HashSet<>(dirtyKeyRecorder);
        dirtyKeyRecorder.removeAll(set);
        return set;
    }

    public Set<Long> getDirtyKeyRecorder() {
        return dirtyKeyRecorder;
    }

    @Override
    public String toString() {
        return rangeSet.toString();
    }

    class LruTask implements Runnable {

        @Override
        public void run() {
            try {
                log.debug("start schedule LruTask, keys in cache:{}", lruCounter.getKeys());
                // remove invalid key which is removed in rangeSet
                Range<T> firstRange = rangeSet.firstRange();
                if (firstRange == null) {
                    return;
                }
                // remove invalid key
                Iterator<Long> iterator = lruCounter.getKeys().iterator();
                while (iterator.hasNext()) {
                    long ledgerId = iterator.next();
                    if (firstRange.lowerEndpoint().compareTo(
                            rangeConverter.apply(ledgerId, Integer.MAX_VALUE - 1)) > 0) {
                        iterator.remove();
                        log.info("LruTask remove invalid key {}", ledgerId);
                    }
                }
                if (isReachLruSwitchThreshold() && lruCounter.size() > 1) {
                    long eldestKey = lruCounter.removeEldestEntryAndGet();
                    log.info("Reach switching threshold, try to remove eldest ledger {}, range size {}",
                            eldestKey, rangeSet.size());
                    if (rangeSet instanceof ConcurrentOpenLongPairRangeSet) {
                        ConcurrentOpenLongPairRangeSet<T> set =
                                (ConcurrentOpenLongPairRangeSet<T>) rangeSet;
                        set.remove(Range.openClosed(new LongPair(eldestKey, 0), new LongPair(eldestKey,
                                Integer.MAX_VALUE - 1)));
                    } else {
                        LongPairRangeSet.DefaultRangeSet<T> set =
                                (LongPairRangeSet.DefaultRangeSet<T>) rangeSet;
                        set.remove(Range.openClosed(rangeConverter.apply(eldestKey, 0),
                                rangeConverter.apply(eldestKey, Integer.MAX_VALUE - 1)));
                    }
                }
            } catch (Exception e) {
                log.error("LruTask run failed", e);
            }
        }
    }

    static class LruCache<K,V> {
        private Field field;
        final LinkedHashMap<K,V> linkedHashMap;

        public LruCache() {
            linkedHashMap = new LinkedHashMap<>(10, 0.75f, true);
        }

        public Set<K> getKeys() {
            return linkedHashMap.keySet();
        }

        public V remove(K key) {
            return linkedHashMap.remove(key);
        }

        public void touch(K key) {
            linkedHashMap.putIfAbsent(key, null);
        }

        public K removeEldestEntryAndGet() {
            Map.Entry<K, V> entry = getEldestEntry();
            if (entry != null) {
                linkedHashMap.remove(entry.getKey());
                return entry.getKey();
            }
            return null;
        }

        public void clear() {
            linkedHashMap.clear();
        }

        public Map.Entry<K, V> getEldestEntry() {
            try {
                if (field == null) {
                    field = LinkedHashMap.class.getDeclaredField("head");
                    field.setAccessible(true);
                }
                return (Map.Entry<K, V>) field.get(linkedHashMap);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public int size() {
            return linkedHashMap.size();
        }
    }
}

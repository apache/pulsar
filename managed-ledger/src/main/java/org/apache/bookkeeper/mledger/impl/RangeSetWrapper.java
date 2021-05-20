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
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RangeSetWrapper<T extends Comparable<T>> implements LongPairRangeSet<T>, AutoCloseable {

    private final LongPairRangeSet<T> rangeSet;
    final LongPairConsumer<T> rangeConverter;
    final ManagedLedgerImpl managedLedger;
    final ManagedLedgerConfig config;
    final ScheduledFuture<?> future;
    final ConcurrentLinkedQueue<Long> pendingTouch = new ConcurrentLinkedQueue<>();
    final LruCache<Long, Byte> lruCounter = new LruCache<>(10, 0.75f, true);

    public RangeSetWrapper(ManagedLedgerImpl managedLedger, LongPairConsumer<T> rangeConverter) {
        this.rangeConverter = rangeConverter;
        this.config = managedLedger.getConfig();
        this.managedLedger = managedLedger;
        this.rangeSet = getLongPairRangeSetImpl();
        future = config.isEnableLruCacheMaxUnackedRanges() ?
                managedLedger.getScheduledExecutor()
                        .scheduleAtFixedRate(new LruTask(this), 1, 1, TimeUnit.SECONDS) : null;
    }

    @Override
    public void addOpenClosed(long lowerKey, long lowerValue, long upperKey, long upperValue) {
        lruTouch(upperKey);
        rangeSet.addOpenClosed(lowerKey, lowerValue, upperKey, upperValue);
    }

    private void lruTouch(long ledgerId) {
        if (config.isEnableLruCacheMaxUnackedRanges()) {
            pendingTouch.offer(ledgerId);
        }
    }

    private boolean isReachSwitchingThreshold() {
        // every position has 3 long properties
        return (long) size() * 24 > config.getMaxUnackedRangesInMemoryBytes();
    }

    private LongPairRangeSet<T>  getLongPairRangeSetImpl() {
        return config.isUnackedRangesOpenCacheSetEnabled()
                ? new ConcurrentOpenLongPairRangeSet<>(4096, rangeConverter)
                : new LongPairRangeSet.DefaultRangeSet<>(rangeConverter);
    }

    @Override
    public boolean contains(long key, long value) {
        lruTouch(key);
        return rangeSet.contains(key, value);
    }

    @Override
    public Range rangeContaining(long key, long value) {
        lruTouch(key);
        return rangeSet.rangeContaining(key, value);
    }

    @Override
    public void removeAtMost(long key, long value) {
        lruTouch(key);
        rangeSet.removeAtMost(key, value);
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
            ConcurrentOpenLongPairRangeSet set = (ConcurrentOpenLongPairRangeSet) rangeSet;
            Range<LongPair> longPairRange = (Range<LongPair>) range;
            set.remove(longPairRange);
        } else {
            LongPairRangeSet.DefaultRangeSet set = (LongPairRangeSet.DefaultRangeSet) rangeSet;
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
        if (future != null) {
            future.cancel(true);
        }
    }

    @Override
    public String toString() {
        return rangeSet.toString();
    }

    private List<Range<PositionImpl>> getPositionRangeInLedger(long ledgerId) {
        LongPairRangeSet<PositionImpl> set = (LongPairRangeSet<PositionImpl>) rangeSet;
        List<Range<PositionImpl>> ranges = new ArrayList<>();
        set.forEach(range -> {
            if (range.upperEndpoint().getLedgerId() == ledgerId) {
                ranges.add(range);
            }
            return true;
        });
        return ranges;
    }

    class LruTask implements Runnable {
        final RangeSetWrapper<PositionImpl> wrapper;

        public LruTask(RangeSetWrapper rangeSetWrapper) {
            this.wrapper = rangeSetWrapper;
        }
        @Override
        public void run() {
            Long key = pendingTouch.poll();
            while (key != null) {
                lruCounter.put(key, null);
                key = pendingTouch.poll();
            }

            if (isReachSwitchingThreshold()) {
                long eldestKey = lruCounter.removeEldestEntryAndGet();
                if (rangeSet instanceof ConcurrentOpenLongPairRangeSet) {
                    ConcurrentOpenLongPairRangeSet<PositionImpl> set =
                            (ConcurrentOpenLongPairRangeSet<PositionImpl>) rangeSet;
                    Range<LongPair> range = Range.openClosed(new LongPair(0, 0), new LongPair(0, 0));
                    for (Range<PositionImpl> positionRange : getPositionRangeInLedger(eldestKey)) {
                        range.lowerEndpoint().setKey(positionRange.lowerEndpoint().ledgerId);
                        range.lowerEndpoint().setValue(positionRange.lowerEndpoint().entryId);
                        range.upperEndpoint().setKey(positionRange.upperEndpoint().ledgerId);
                        range.upperEndpoint().setValue(positionRange.upperEndpoint().entryId);
                        set.remove(range);
                    }
                } else {
                    LongPairRangeSet.DefaultRangeSet<PositionImpl> set =
                            (LongPairRangeSet.DefaultRangeSet<PositionImpl>) rangeSet;
                    for (Range<PositionImpl> positionRange : getPositionRangeInLedger(eldestKey)) {
                        set.remove(positionRange);
                    }
                }
            }
        }
    }

    static class LruCache<K,V> extends LinkedHashMap<K,V> {
        Map.Entry<K,V> eldestEntry;

        public LruCache(int initialCapacity,
                        float loadFactor,
                        boolean accessOrder) {
            super(initialCapacity, loadFactor, accessOrder);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
            eldestEntry = eldest;
            return false;
        }

        public K removeEldestEntryAndGet() {
            K key = eldestEntry.getKey();
            super.remove(key);
            return key;
        }

        public Map.Entry<K, V> getEldestEntry() {
            return eldestEntry;
        }
    }
}

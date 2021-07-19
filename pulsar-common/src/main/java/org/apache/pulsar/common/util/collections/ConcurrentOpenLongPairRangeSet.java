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
package org.apache.pulsar.common.util.collections;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Concurrent set comprising zero or more ranges of type {@link LongPair}. This can be alternative of
 * {@link com.google.common.collect.RangeSet} and can be used if {@code range} type is {@link LongPair}
 *
 * <pre>
 * Usage:
 * a. This can be used if one doesn't want to create object for every new inserted {@code range}
 * b. It creates {@link BitSet} for every unique first-key of the range.
 * So, this rangeSet is not suitable for large number of unique keys.
 * </pre>
 */
public class ConcurrentOpenLongPairRangeSet<T extends Comparable<T>> implements LongPairRangeSet<T> {

    protected final NavigableMap<Long, BitSet> rangeBitSetMap = new ConcurrentSkipListMap<>();
    private boolean threadSafe = true;
    private final int bitSetSize;
    private final LongPairConsumer<T> consumer;

    // caching place-holder for cpu-optimization to avoid calculating ranges again
    private volatile int cachedSize = 0;
    private volatile String cachedToString = "[]";
    private volatile boolean updatedAfterCachedForSize = true;
    private volatile boolean updatedAfterCachedForToString = true;

    public ConcurrentOpenLongPairRangeSet(LongPairConsumer<T> consumer) {
        this(1024, true, consumer);
    }

    public ConcurrentOpenLongPairRangeSet(int size, LongPairConsumer<T> consumer) {
        this(size, true, consumer);
    }

    public ConcurrentOpenLongPairRangeSet(int size, boolean threadSafe, LongPairConsumer<T> consumer) {
        this.threadSafe = threadSafe;
        this.bitSetSize = size;
        this.consumer = consumer;
    }

    /**
     * Adds the specified range to this {@code RangeSet} (optional operation). That is, for equal range sets a and b,
     * the result of {@code a.add(range)} is that {@code a} will be the minimal range set for which both
     * {@code a.enclosesAll(b)} and {@code a.encloses(range)}.
     *
     * <p>Note that {@code range} will merge given {@code range} with any ranges in the range set that are
     * {@linkplain Range#isConnected(Range) connected} with it. Moreover, if {@code range} is empty, this is a no-op.
     */
    @Override
    public void addOpenClosed(long lowerKey, long lowerValueOpen, long upperKey, long upperValue) {
        long lowerValue = lowerValueOpen + 1;
        if (lowerKey != upperKey) {
            // (1) set lower to last in lowerRange.getKey()
            if (isValid(lowerKey, lowerValue)) {
                BitSet rangeBitSet = rangeBitSetMap.get(lowerKey);
                // if lower and upper has different key/ledger then set ranges for lower-key only if
                // a. bitSet already exist and given value is not the last value in the bitset.
                // it will prevent setting up values which are not actually expected to set
                // eg: (2:10..4:10] in this case , don't set any value for 2:10 and set [4:0..4:10]
                if (rangeBitSet != null && (rangeBitSet.previousSetBit(rangeBitSet.size()) > lowerValueOpen)) {
                    int lastValue = rangeBitSet.previousSetBit(rangeBitSet.size());
                    rangeBitSet.set((int) lowerValue, (int) Math.max(lastValue, lowerValue) + 1);
                }
            }
            // (2) set 0th-index to upper-index in upperRange.getKey()
            if (isValid(upperKey, upperValue)) {
                BitSet rangeBitSet = rangeBitSetMap.computeIfAbsent(upperKey, (key) -> createNewBitSet());
                if (rangeBitSet != null) {
                    rangeBitSet.set(0, (int) upperValue + 1);
                }
            }
            // No-op if values are not valid eg: if lower == LongPair.earliest or upper == LongPair.latest then nothing
            // to set
        } else {
            long key = lowerKey;
            BitSet rangeBitSet = rangeBitSetMap.computeIfAbsent(key, (k) -> createNewBitSet());
            rangeBitSet.set((int) lowerValue, (int) upperValue + 1);
        }
        updatedAfterCachedForSize = true;
        updatedAfterCachedForToString = true;
    }

    private boolean isValid(long key, long value) {
        return key != LongPair.earliest.getKey() && value != LongPair.earliest.getValue()
                && key != LongPair.latest.getKey() && value != LongPair.latest.getValue();
    }

    @Override
    public boolean contains(long key, long value) {

        BitSet rangeBitSet = rangeBitSetMap.get(key);
        if (rangeBitSet != null) {
            return rangeBitSet.get(getSafeEntry(value));
        }
        return false;
    }

    @Override
    public Range<T> rangeContaining(long key, long value) {
        BitSet rangeBitSet = rangeBitSetMap.get(key);
        if (rangeBitSet != null) {
            if (!rangeBitSet.get(getSafeEntry(value))) {
                // if position is not part of any range then return null
                return null;
            }
            int lowerValue = rangeBitSet.previousClearBit(getSafeEntry(value)) + 1;
            final T lower = consumer.apply(key, lowerValue);
            final T upper = consumer.apply(key,
                    Math.max(rangeBitSet.nextClearBit(getSafeEntry(value)) - 1, lowerValue));
            return Range.closed(lower, upper);
        }
        return null;
    }

    @Override
    public void removeAtMost(long key, long value) {
        this.remove(Range.atMost(new LongPair(key, value)));
    }

    @Override
    public boolean isEmpty() {
        if (rangeBitSetMap.isEmpty()) {
            return true;
        }
        AtomicBoolean isEmpty = new AtomicBoolean(false);
        rangeBitSetMap.forEach((key, val) -> {
            if (!isEmpty.get()) {
                return;
            }
            isEmpty.set(val.isEmpty());
        });
        return isEmpty.get();
    }

    @Override
    public void clear() {
        rangeBitSetMap.clear();
        updatedAfterCachedForSize = true;
        updatedAfterCachedForToString = true;
    }

    @Override
    public Range<T> span() {
        if (rangeBitSetMap.size() == 0) {
            return null;
        }
        Entry<Long, BitSet> firstSet = rangeBitSetMap.firstEntry();
        Entry<Long, BitSet> lastSet = rangeBitSetMap.lastEntry();
        int first = firstSet.getValue().nextSetBit(0);
        int last = lastSet.getValue().previousSetBit(lastSet.getValue().size());
        return Range.openClosed(consumer.apply(firstSet.getKey(), first - 1), consumer.apply(lastSet.getKey(), last));
    }

    @Override
    public List<Range<T>> asRanges() {
        List<Range<T>> ranges = new ArrayList<>();
        forEach((range) -> {
            ranges.add(range);
            return true;
        });
        return ranges;
    }

    @Override
    public void forEach(RangeProcessor<T> action) {
        forEach(action, consumer);
    }

    @Override
    public void forEach(RangeProcessor<T> action, LongPairConsumer<? extends T> consumer) {
        AtomicBoolean completed = new AtomicBoolean(false);
        rangeBitSetMap.forEach((key, set) -> {
            if (completed.get()) {
                return;
            }
            if (set.isEmpty()) {
                return;
            }
            int first = set.nextSetBit(0);
            int last = set.previousSetBit(set.size());
            int currentClosedMark = first;
            while (currentClosedMark != -1 && currentClosedMark <= last) {
                int nextOpenMark = set.nextClearBit(currentClosedMark);
                Range<T> range = Range.openClosed(consumer.apply(key, currentClosedMark - 1),
                        consumer.apply(key, nextOpenMark - 1));
                if (!action.process(range)) {
                    completed.set(true);
                    break;
                }
                currentClosedMark = set.nextSetBit(nextOpenMark);
            }
        });
    }

    @Override
    public Range<T> firstRange() {
        if (rangeBitSetMap.isEmpty()) {
            return null;
        }
        Entry<Long, BitSet> firstSet = rangeBitSetMap.firstEntry();
        int lower = firstSet.getValue().nextSetBit(0);
        int upper = Math.max(lower, firstSet.getValue().nextClearBit(lower) - 1);
        return Range.openClosed(consumer.apply(firstSet.getKey(), lower - 1), consumer.apply(firstSet.getKey(), upper));
    }

    @Override
    public Range<T> lastRange() {
        if (rangeBitSetMap.isEmpty()) {
            return null;
        }
        Entry<Long, BitSet> lastSet = rangeBitSetMap.lastEntry();
        int upper = lastSet.getValue().previousSetBit(lastSet.getValue().size());
        int lower = Math.min(lastSet.getValue().previousClearBit(upper), upper);
        return Range.openClosed(consumer.apply(lastSet.getKey(), lower), consumer.apply(lastSet.getKey(), upper));
    }

    @Override
    public int size() {
        if (updatedAfterCachedForSize) {
            AtomicInteger size = new AtomicInteger(0);
            forEach((range) -> {
                size.getAndIncrement();
                return true;
            });
            cachedSize = size.get();
            updatedAfterCachedForSize = false;
        }
        return cachedSize;
    }

    @Override
    public String toString() {
        if (updatedAfterCachedForToString) {
            StringBuilder toString = new StringBuilder();
            AtomicBoolean first = new AtomicBoolean(true);
            if (toString != null) {
                toString.append("[");
            }
            forEach((range) -> {
                if (!first.get()) {
                    toString.append(",");
                }
                toString.append(range);
                first.set(false);
                return true;
            });
            toString.append("]");
            cachedToString = toString.toString();
            updatedAfterCachedForToString = false;
        }
        return cachedToString;
    }

    /**
     * Adds the specified range to this {@code RangeSet} (optional operation). That is, for equal range sets a and b,
     * the result of {@code a.add(range)} is that {@code a} will be the minimal range set for which both
     * {@code a.enclosesAll(b)} and {@code a.encloses(range)}.
     *
     * <p>Note that {@code range} will merge given {@code range} with any ranges in the range set that are
     * {@linkplain Range#isConnected(Range) connected} with it. Moreover, if {@code range} is empty/invalid, this is a
     * no-op.
     */
    public void add(Range<LongPair> range) {
        LongPair lowerEndpoint = range.hasLowerBound() ? range.lowerEndpoint() : LongPair.earliest;
        LongPair upperEndpoint = range.hasUpperBound() ? range.upperEndpoint() : LongPair.latest;

        long lowerValueOpen = (range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(lowerEndpoint) - 1
                : getSafeEntry(lowerEndpoint);
        long upperValueClosed = (range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(upperEndpoint)
                : getSafeEntry(upperEndpoint) + 1;

        // #addOpenClosed doesn't create bitSet for lower-key because it avoids setting up values for non-exist items
        // into the key-ledger. so, create bitSet and initialize so, it can't be ignored at #addOpenClosed
        rangeBitSetMap.computeIfAbsent(lowerEndpoint.getKey(), (key) -> createNewBitSet())
                .set((int) lowerValueOpen + 1);
        this.addOpenClosed(lowerEndpoint.getKey(), lowerValueOpen, upperEndpoint.getKey(), upperValueClosed);
    }

    public boolean contains(LongPair position) {
        checkNotNull(position, "argument can't be null");
        return contains(position.getKey(), position.getValue());
    }

    public void remove(Range<LongPair> range) {
        LongPair lowerEndpoint = range.hasLowerBound() ? range.lowerEndpoint() : LongPair.earliest;
        LongPair upperEndpoint = range.hasUpperBound() ? range.upperEndpoint() : LongPair.latest;

        long lower = (range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(lowerEndpoint)
                : getSafeEntry(lowerEndpoint) + 1;
        long upper = (range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(upperEndpoint)
                : getSafeEntry(upperEndpoint) - 1;

        // if lower-bound is not set then remove all the keys less than given upper-bound range
        if (lowerEndpoint.equals(LongPair.earliest)) {
            // remove all keys with
            rangeBitSetMap.forEach((key, set) -> {
                if (key < upperEndpoint.getKey()) {
                    rangeBitSetMap.remove(key);
                }
            });
        }

        // if upper-bound is not set then remove all the keys greater than given lower-bound range
        if (upperEndpoint.equals(LongPair.latest)) {
            // remove all keys with
            rangeBitSetMap.forEach((key, set) -> {
                if (key > lowerEndpoint.getKey()) {
                    rangeBitSetMap.remove(key);
                }
            });
        }

        // remove all the keys between two endpoint keys
        rangeBitSetMap.forEach((key, set) -> {
            if (lowerEndpoint.getKey() == upperEndpoint.getKey() && key == upperEndpoint.getKey()) {
                set.clear((int) lower, (int) upper + 1);
            } else {
                // eg: remove-range: [(3,5) - (5,5)] -> Delete all items from 3,6->3,N,4.*,5,0->5,5
                if (key == lowerEndpoint.getKey()) {
                    // remove all entries from given position to last position
                    set.clear((int) lower, set.previousSetBit(set.size()));
                } else if (key == upperEndpoint.getKey()) {
                    // remove all entries from 0 to given position
                    set.clear(0, (int) upper + 1);
                } else if (key > lowerEndpoint.getKey() && key < upperEndpoint.getKey()) {
                    rangeBitSetMap.remove(key);
                }
            }
            // remove bit-set if set is empty
            if (set.isEmpty()) {
                rangeBitSetMap.remove(key);
            }
        });

        updatedAfterCachedForSize = true;
        updatedAfterCachedForToString = true;
    }

    private int getSafeEntry(LongPair position) {
        return (int) Math.max(position.getValue(), -1);
    }

    private int getSafeEntry(long value) {
        return (int) Math.max(value, -1);
    }

    private BitSet createNewBitSet() {
        return this.threadSafe ? new ConcurrentBitSet(bitSetSize) : new BitSet(bitSetSize);
    }

}

package org.apache.pulsar.common.util.collections;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.BoundType;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Range;

/**
 * A Concurrent set comprising zero or more ranges of type {@link LongPair}. This can be alternative of
 * {@link com.google.common.collect.RangeSet} and can be used if {@code range} type is {@link LongPair} </br>
 * 
 * <pre>
 *  
 * Usage:
 * a. This can be used if one doesn't want to create object for every new inserted {@code range}
 * b. It creates {@link BitSet} for every unique first-key of the range. 
 * So, this rangeSet is not appropriate for large number of unique first-keys.
 * </pre>
 * 
 *
 */
public class ConcurrentLongPairRangeSet implements LongPairRangeSet {

    protected final NavigableMap<Long, BitSet> rangeBitSetMap = new ConcurrentSkipListMap<>();
    private boolean threadSafe = true;
    private final int size;

    public ConcurrentLongPairRangeSet() {
        this(1024, true);
    }

    public ConcurrentLongPairRangeSet(int size) {
        this(size, true);
    }

    public ConcurrentLongPairRangeSet(int size, boolean threadSafe) {
        this.threadSafe = threadSafe;
        this.size = size;
    }

    public void clear() {
        rangeBitSetMap.clear();
    }

    class ConcurrentBitSet extends BitSet {
        private static final long serialVersionUID = 1L;
        private final StampedLock rwLock = new StampedLock();

        /**
         * Creates a bit set whose initial size is large enough to explicitly represent bits with indices in the range
         * {@code 0} through {@code nbits-1}. All bits are initially {@code false}.
         *
         * @param nbits
         *            the initial size of the bit set
         * @throws NegativeArraySizeException
         *             if the specified initial size is negative
         */
        public ConcurrentBitSet(int nbits) {
            super(nbits);
        }

        @Override
        public boolean get(int bitIndex) {
            return super.get(bitIndex);
        }

        @Override
        public void set(int bitIndex) {
            long stamp = rwLock.writeLock();
            try {
                super.set(bitIndex);
            } finally {
                rwLock.unlockWrite(stamp);
            }
        }

        @Override
        public void set(int fromIndex, int toIndex) {
            long stamp = rwLock.writeLock();
            try {
                super.set(fromIndex, toIndex);
            } finally {
                rwLock.unlockWrite(stamp);
            }
        }

        @Override
        public int nextSetBit(int fromIndex) {
            long stamp = rwLock.readLock();
            try {
                return super.nextSetBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }

        @Override
        public int nextClearBit(int fromIndex) {
            long stamp = rwLock.readLock();
            try {
                return super.nextClearBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }

        @Override
        public int previousSetBit(int fromIndex) {
            long stamp = rwLock.readLock();
            try {
                return super.previousSetBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }

        @Override
        public int previousClearBit(int fromIndex) {
            long stamp = rwLock.readLock();
            try {
                return super.previousClearBit(fromIndex);
            } finally {
                rwLock.unlockRead(stamp);
            }
        }

        @Override
        public boolean isEmpty() {
            long stamp = rwLock.tryOptimisticRead();
            boolean isEmpty = super.isEmpty();
            if (!rwLock.validate(stamp)) {
                // Fallback to read lock
                stamp = rwLock.readLock();
                try {
                    isEmpty = super.isEmpty();
                } finally {
                    rwLock.unlockRead(stamp);
                }
            }
            return isEmpty;
        }

    }

    /**
     * Adds the specified range to this {@code RangeSet} (optional operation). That is, for equal range sets a and b,
     * the result of {@code a.add(range)} is that {@code a} will be the minimal range set for which both
     * {@code a.enclosesAll(b)} and {@code a.encloses(range)}.
     *
     * <p>
     * Note that {@code range} will merge given {@code range} with any ranges in the range set that are
     * {@linkplain Range#isConnected(Range) connected} with it. Moreover, if {@code range} is empty, this is a no-op.
     */
    public void add(Range<LongPair> range) {
        LongPair lowerEndpoint = range.hasLowerBound() ? range.lowerEndpoint() : LongPair.earliest;
        LongPair upperEndpoint = range.hasUpperBound() ? range.upperEndpoint() : LongPair.latest;

        int lower = (range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(lowerEndpoint)
                : getSafeEntry(lowerEndpoint) + 1;
        int upper = (range.hasUpperBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(upperEndpoint)
                : getSafeEntry(upperEndpoint) + 1;
        if (lowerEndpoint.getKey() != upperEndpoint.getKey()) {
            // (1) set lower to last in lowerRange.getKey()
            if (!lowerEndpoint.equals(LongPair.earliest)) {
                BitSet rangeBitSet = rangeBitSetMap.computeIfAbsent(lowerEndpoint.getKey(), (key) -> createNewBitSet());
                if (rangeBitSet != null) {
                    int lastValue = rangeBitSet.previousSetBit(rangeBitSet.size());
                    rangeBitSet.set(lower, Math.max(lastValue, lower) + 1);
                }
            }
            // (2) set 0 to upper in upperRange.getKey()
            if (!upperEndpoint.equals(LongPair.latest)) {
                BitSet rangeBitSet = rangeBitSetMap.computeIfAbsent(upperEndpoint.getKey(), (key) -> createNewBitSet());
                if (rangeBitSet != null) {
                    rangeBitSet.set(0, upper + 1);
                }
            }
        } else {
            long key = lowerEndpoint.getKey();
            BitSet rangeBitSet = rangeBitSetMap.computeIfAbsent(key, (k) -> createNewBitSet());
            rangeBitSet.set(lower, upper + 1);
        }
    }

    public boolean contains(LongPair position) {
        checkNotNull(position, "argument can't be null");
        BitSet rangeBitSet = rangeBitSetMap.get(position.getKey());
        if (rangeBitSet != null) {
            return rangeBitSet.get(getSafeEntry(position));
        }
        return false;
    }

    public Range<LongPair> rangeContaining(LongPair position) {
        checkNotNull(position, "argument can't be null");
        BitSet rangeBitSet = rangeBitSetMap.get(position.getKey());
        if (rangeBitSet != null) {
            if (!rangeBitSet.get(getSafeEntry(position))) {
                // TODO: document it
                // if position is not part of any range then return null
                return null;
            }
            final LongPair lower = new LongPair(position.getKey(),
                    rangeBitSet.previousClearBit(getSafeEntry(position)) + 1);
            final LongPair upper = new LongPair(position.getKey(),
                    Math.max(rangeBitSet.nextClearBit(getSafeEntry(position)) - 1, lower.getValue()));
            return Range.closed(lower, upper);
        } else {
            // position's key doesn't exist so, range should be last entry Of previous key and first entry of next
            // key
            Entry<Long, BitSet> previousRangeBitSet = rangeBitSetMap.ceilingEntry(position.getKey());
            final LongPair lower = (previousRangeBitSet != null)
                    ? new LongPair(previousRangeBitSet.getKey(),
                            previousRangeBitSet.getValue().previousSetBit(previousRangeBitSet.getValue().size()))
                    : LongPair.earliest;
            Entry<Long, BitSet> nextRangeBitSet = rangeBitSetMap.floorEntry(position.getKey());
            final LongPair upper = (nextRangeBitSet != null)
                    ? new LongPair(nextRangeBitSet.getKey(), nextRangeBitSet.getValue().nextSetBit(0))
                    : LongPair.latest;
            return Range.closed(lower, upper);
        }
    }

    public void remove(Range<LongPair> range) {
        LongPair lowerEndpoint = range.hasLowerBound() ? range.lowerEndpoint() : LongPair.earliest;
        LongPair upperEndpoint = range.hasUpperBound() ? range.upperEndpoint() : LongPair.latest;

        int lower = (range.hasLowerBound() && range.lowerBoundType().equals(BoundType.CLOSED))
                ? getSafeEntry(lowerEndpoint)
                : getSafeEntry(lowerEndpoint) + 1;
        int upper = (range.hasUpperBound() && range.upperBoundType().equals(BoundType.CLOSED))
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
            if (lowerEndpoint.getKey() == upperEndpoint.getKey()) {
                set.clear(lower, upper + 1);
            } else {
                // eg: remove-range: [(3,5) - (5,5)] -> Delete all items from 3,6->3,N,4.*,5,0->5,5
                if (key == lowerEndpoint.getKey()) {
                    // remove all entries from given position to last position
                    set.clear(lower, set.previousSetBit(set.size()));
                } else if (key == upperEndpoint.getKey()) {
                    // remove all entries from 0 to given position
                    set.clear(0, upper + 1);
                } else if (key > lowerEndpoint.getKey() && key < upperEndpoint.getKey()) {
                    rangeBitSetMap.remove(key);
                }
            }
            // remove bit-set if set is empty
            if (set.isEmpty()) {
                rangeBitSetMap.remove(key);
            }
        });
    }

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

    public Range<LongPair> span() {
        Entry<Long, BitSet> firstSet = rangeBitSetMap.firstEntry();
        Entry<Long, BitSet> lastSet = rangeBitSetMap.lastEntry();
        int first = firstSet.getValue().nextSetBit(0);
        int last = lastSet.getValue().previousSetBit(lastSet.getValue().size());
        return Range.closed(new LongPair(firstSet.getKey(), first), new LongPair(lastSet.getKey(), last));
    }

    public List<Range<LongPair>> asRanges() {
        List<Range<LongPair>> ranges = new ArrayList<>();
        rangeBitSetMap.forEach((key, set) -> {
            if (set.isEmpty()) {
                return;
            }
            int first = set.nextSetBit(0);
            int last = set.previousSetBit(set.size());
            int currentClosedMark = first;
            while (currentClosedMark != -1 && currentClosedMark <= last) {
                int nextOpenMark = set.nextClearBit(currentClosedMark);
                Range<LongPair> range = Range.closed(new LongPair(key, currentClosedMark),
                        new LongPair(key, nextOpenMark - 1));
                ranges.add(range);
                currentClosedMark = set.nextSetBit(nextOpenMark);
            }
        });
        return ranges;
    }

    private int getSafeEntry(LongPair position) {
        return (int) (position.getValue() > 0 ? position.getValue() : 0);
    }

    private BitSet createNewBitSet() {
        return this.threadSafe ? new ConcurrentBitSet(size) : new BitSet(size);
    }

}

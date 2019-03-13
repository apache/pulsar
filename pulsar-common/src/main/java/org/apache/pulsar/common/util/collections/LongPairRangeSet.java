package org.apache.pulsar.common.util.collections;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * A set comprising zero or more ranges type of {@link LongPair}
 */
public interface LongPairRangeSet {

    /**
     * Default RangeSet implementation based on {@link TreeRangeSet}
     * 
     * @return
     */
    static LongPairRangeSet create() {
        return new DefaultRangeSet();
    }

    void clear();

    /**
     * Adds the specified range to this {@code RangeSet} (optional operation). That is, for equal range sets a and b,
     * the result of {@code a.add(range)} is that {@code a} will be the minimal range set for which both
     * {@code a.enclosesAll(b)} and {@code a.encloses(range)}.
     *
     * <p>
     * Note that {@code range} will merge given {@code range} with any ranges in the range set that are
     * {@linkplain Range#isConnected(Range) connected} with it. Moreover, if {@code range} is empty, this is a no-op.
     */
    void add(Range<LongPair> range);

    /** Determines whether any of this range set's member ranges contains {@code value}. */
    boolean contains(LongPair position);

    /**
     * Returns the unique range from this range set that {@linkplain Range#contains contains} {@code value}, or
     * {@code null} if this range set does not contain {@code value}.
     */
    Range<LongPair> rangeContaining(LongPair position);

    /**
     * Removes the specified range from this {@code RangeSet}
     * 
     * @param range
     */
    void remove(Range<LongPair> range);

    boolean isEmpty();

    /**
     * Returns the minimal range which {@linkplain Range#encloses(Range) encloses} all ranges in this range set.
     * 
     * @return
     */
    Range<LongPair> span();

    /**
     * Returns a view of the {@linkplain Range#isConnected disconnected} ranges that make up this range set.
     * 
     * @return
     */
    Collection<Range<LongPair>> asRanges();

    public static class LongPair implements Comparable<LongPair> {

        public static final LongPair earliest = new LongPair(-1, -1);
        public static final LongPair latest = new LongPair(Integer.MAX_VALUE, Integer.MAX_VALUE);

        private long key;
        private int value;

        public LongPair(long key, int value) {
            this.key = key;
            this.value = value;
        }

        public long getKey() {
            return this.key;
        }

        public int getValue() {
            return this.value;
        }

        @Override
        public int compareTo(LongPair o) {
            return ComparisonChain.start().compare(key, o.getKey()).compare(value, o.getValue()).result();
        }

        @Override
        public String toString() {
            return String.format("%d:%d", key, value);
        }
    }

    public static class DefaultRangeSet implements LongPairRangeSet {

        RangeSet<LongPair> set = TreeRangeSet.create();

        @Override
        public void clear() {
            set.clear();
        }

        @Override
        public void add(Range<LongPair> range) {
            set.add(range);
        }

        @Override
        public boolean contains(LongPair position) {
            return set.contains(position);
        }

        @Override
        public Range<LongPair> rangeContaining(LongPair position) {
            return set.rangeContaining(position);
        }

        @Override
        public void remove(Range<LongPair> range) {
            set.remove(range);
        }

        @Override
        public boolean isEmpty() {
            return set.isEmpty();
        }

        @Override
        public Range<LongPair> span() {
            return set.span();
        }

        @Override
        public Set<Range<LongPair>> asRanges() {
            return set.asRanges();
        }
    }
}

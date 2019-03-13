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

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import org.apache.pulsar.common.util.collections.ConcurrentLongPairRangeSet.LongPair;
import org.testng.annotations.Test;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;

public class ConcurrentLongPairRangeSetTest {

    @Test
    public void testAddForSameKey() {
        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        // add 0 to 5
        set.add(Range.closed(new LongPair(0, 0), new LongPair(0, 5)));
        // add 8,9,10
        set.add(Range.closed(new LongPair(0, 8), new LongPair(0, 8)));
        set.add(Range.closed(new LongPair(0, 9), new LongPair(0, 9)));
        set.add(Range.closed(new LongPair(0, 10), new LongPair(0, 10)));
        // add 98 to 99 and 102,105
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 102), new LongPair(0, 106)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(0, 0), new LongPair(0, 5))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(0, 8), new LongPair(0, 10))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(0, 98), new LongPair(0, 99))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(0, 102), new LongPair(0, 106))));
    }

    @Test
    public void testAddForDifferentKey() {
        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        // [98,100],[(1,5),(1,5)],[(1,10,1,15)],[(1,20),(1,20)],[(2,0),(2,10)]
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(0, 98), new LongPair(0, 100))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 0), new LongPair(1, 5))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 10), new LongPair(1, 15))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 20), new LongPair(1, 20))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(2, 0), new LongPair(2, 10))));
    }

    @Test
    public void testAddCompareCompareWithGuava() {
        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        com.google.common.collect.RangeSet<LongPair> gSet = TreeRangeSet.create();

        // add 10K values for key 0
        int totalInsert = 10_000;
        // add single values
        for (int i = 0; i < totalInsert; i++) {
            if (i % 3 == 0 || i % 6 == 0 || i % 8 == 0) {
                set.add(Range.closed(new LongPair(0, i), new LongPair(0, i)));
                gSet.add(Range.closed(new LongPair(0, i), new LongPair(0, i)));
            }
        }
        // add batches
        for (int i = totalInsert; i < (totalInsert * 2); i++) {
            if (i % 5 == 0) {
                set.add(Range.closed(new LongPair(0, i - 3), new LongPair(0, i + 3)));
                gSet.add(Range.closed(new LongPair(0, i - 3), new LongPair(0, i + 3)));
            }
        }
        List<Range<LongPair>> ranges = set.asRanges();
        Set<Range<LongPair>> gRanges = gSet.asRanges();

        List<Range<LongPair>> gRangeConnected = getConnectedRange(gRanges);
        assertEquals(gRangeConnected.size(), ranges.size());
        int i = 0;
        for (Range<LongPair> range : gRangeConnected) {
            assertEquals(range, ranges.get(i));
            i++;
        }
    }

    @Test
    public void testDeleteCompareWithGuava() {

        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        com.google.common.collect.RangeSet<LongPair> gSet = TreeRangeSet.create();

        // add 10K values for key 0
        int totalInsert = 10_000;
        // add single values
        List<Range<LongPair>> removedRanges = Lists.newArrayList();
        for (int i = 0; i < totalInsert; i++) {
            if (i % 3 == 0 || i % 7 == 0 || i % 11 == 0) {
                continue;
            }
            Range<LongPair> range = Range.closed(new LongPair(0, i), new LongPair(0, i));
            set.add(range);
            gSet.add(range);
            if (i % 4 == 0) {
                removedRanges.add(range);
            }
        }
        // add batches
        for (int i = totalInsert; i < (totalInsert * 2); i++) {
            Range<LongPair> range = Range.closed(new LongPair(0, i - 3), new LongPair(0, i + 3));
            if (i % 5 != 0) {
                set.add(range);
                gSet.add(range);
            }
            if (i % 4 == 0) {
                removedRanges.add(range);
            }
        }
        // remove records
        for (Range<LongPair> range : removedRanges) {
            set.remove(range);
            gSet.remove(range);
        }

        List<Range<LongPair>> ranges = set.asRanges();
        Set<Range<LongPair>> gRanges = gSet.asRanges();
        List<Range<LongPair>> gRangeConnected = getConnectedRange(gRanges);
        assertEquals(gRangeConnected.size(), ranges.size());
        int i = 0;
        for (Range<LongPair> range : gRangeConnected) {
            assertEquals(range, ranges.get(i));
            i++;
        }
    }

    @Test
    public void testSpanWithGuava() {
        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        com.google.common.collect.RangeSet<LongPair> gSet = TreeRangeSet.create();
        gSet.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        gSet.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        assertEquals(set.span(), gSet.span());
        assertEquals(set.span(), Range.closed(new LongPair(0, 98), new LongPair(1, 5)));

        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        set.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        set.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        set.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));
        gSet.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        gSet.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        gSet.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        gSet.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        gSet.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));
        assertEquals(set.span(), gSet.span());
        assertEquals(set.span(), Range.closed(new LongPair(0, 98), new LongPair(4, 20)));

    }

    @Test
    public void testDeleteForDifferentKey() {
        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        set.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        set.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        set.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));

        // delete only (0,100)
        set.remove(Range.open(new LongPair(0, 99), new LongPair(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.closed(new LongPair(2, 27), new LongPair(4, 15)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(0, 98), new LongPair(0, 99))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 0), new LongPair(1, 5))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 10), new LongPair(1, 15))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 20), new LongPair(1, 20))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(2, 0), new LongPair(2, 10))));

        assertEquals(ranges.get(count++), (Range.closed(new LongPair(2, 25), new LongPair(2, 26))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(2, 28), new LongPair(2, 28))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(4, 16), new LongPair(4, 20))));
    }

    @Test
    public void testDeleteWithAtMost() {
        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        set.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        set.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        set.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));

        // delete only (0,100)
        set.remove(Range.open(new LongPair(0, 99), new LongPair(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.atMost(new LongPair(2, 27)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(2, 28), new LongPair(2, 28))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(3, 12), new LongPair(3, 20))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(4, 12), new LongPair(4, 20))));
    }

    @Test
    public void testDeleteWithLeastMost() {
        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        set.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        set.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        set.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));

        // delete only (0,100)
        set.remove(Range.open(new LongPair(0, 99), new LongPair(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.atLeast(new LongPair(2, 27)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(0, 98), new LongPair(0, 99))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 0), new LongPair(1, 5))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 10), new LongPair(1, 15))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(1, 20), new LongPair(1, 20))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(2, 0), new LongPair(2, 10))));
        assertEquals(ranges.get(count++), (Range.closed(new LongPair(2, 25), new LongPair(2, 26))));
    }

    @Test
    public void testRangeContaining() {
        ConcurrentLongPairRangeSet set = new ConcurrentLongPairRangeSet();
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        com.google.common.collect.RangeSet<LongPair> gSet = TreeRangeSet.create();
        gSet.add(Range.closed(new LongPair(0, 98), new LongPair(0, 100)));
        gSet.add(Range.closed(new LongPair(0, 101), new LongPair(1, 5)));
        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        set.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        set.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        set.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));
        gSet.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        gSet.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        gSet.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        gSet.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        gSet.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));

        LongPair position = new LongPair(0, 99);
        assertEquals(set.rangeContaining(position), Range.closed(new LongPair(0, 98), new LongPair(0, 100)));
        assertEquals(set.rangeContaining(position), gSet.rangeContaining(position));

        position = new LongPair(2, 30);
        assertEquals(set.rangeContaining(position), null);
        assertEquals(set.rangeContaining(position), gSet.rangeContaining(position));

        position = new LongPair(3, 13);
        assertEquals(set.rangeContaining(position), Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        assertEquals(set.rangeContaining(position), gSet.rangeContaining(position));

        position = new LongPair(3, 22);
        assertEquals(set.rangeContaining(position), null);
        assertEquals(set.rangeContaining(position), gSet.rangeContaining(position));
    }

    private List<Range<LongPair>> getConnectedRange(Set<Range<LongPair>> gRanges) {
        List<Range<LongPair>> gRangeConnected = Lists.newArrayList();
        Range<LongPair> lastRange = null;
        for (Range<LongPair> range : gRanges) {
            if (lastRange == null) {
                lastRange = range;
                continue;
            }
            if ((lastRange.upperEndpoint().getValue() + 1) == (range.lowerEndpoint().getValue())) {
                lastRange = Range.closed(lastRange.lowerEndpoint(), range.upperEndpoint());
            } else {
                gRangeConnected.add(lastRange);
                lastRange = range;
            }
        }
        lastRange = lastRange.lowerBoundType().equals(BoundType.CLOSED) ? lastRange
                : Range.closed(
                        new LongPair(lastRange.lowerEndpoint().getKey(), lastRange.lowerEndpoint().getValue() + 1),
                        lastRange.upperEndpoint());
        gRangeConnected.add(lastRange);
        return gRangeConnected;
    }
}

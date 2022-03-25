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
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

/**
 * Create stats buckets to have frequency distribution of samples.
 *
 */
public class StatsBuckets {
    private final long[] boundaries;
    private final LongAdder sumCounter;
    private final LongAdder[] buckets;

    private final long[] values;
    private long count = 0;
    private long sum = 0;

    public StatsBuckets(long... boundaries) {
        checkArgument(boundaries.length > 0);
        checkArgument(isSorted(boundaries), "Boundaries array must be sorted");
        this.boundaries = boundaries;
        this.sumCounter = new LongAdder();
        this.buckets = new LongAdder[boundaries.length + 1];

        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }

        this.values = new long[buckets.length];
    }

    public void addValue(long value) {
        int idx = Arrays.binarySearch(boundaries, value);
        if (idx < 0) {
            // If the precise value is not found, binary search is returning the negated insertion point
            idx = ~idx;
        }

        buckets[idx].increment();
        sumCounter.add(value);
    }

    public void refresh() {
        long count = 0;
        sum = sumCounter.sumThenReset();

        for (int i = 0; i < buckets.length; i++) {
            long value = buckets[i].sumThenReset();
            count += value;
            values[i] = value;
        }

        this.count = count;
    }

    public void reset() {
        sum = 0;
        sumCounter.reset();
        count = 0;

        for (int i = 0; i < buckets.length; i++) {
            buckets[i].reset();
            values[i] = 0;
        }
    }

    public long[] getBuckets() {
        return values;
    }

    public long getCount() {
        return count;
    }

    public long getSum() {
        return sum;
    }

    public double getAvg() {
        return sum / (double) count;
    }

    public void addAll(StatsBuckets other) {
        checkArgument(boundaries.length == other.boundaries.length,
                "boundaries size %s doesn't match with given boundaries size %s", boundaries.length,
                other.boundaries.length);

        for (int i = 0; i < buckets.length; i++) {
            buckets[i].add(other.values[i]);
        }

        sumCounter.add(other.sum);
    }

    private boolean isSorted(long[] array) {
        long previous = Long.MIN_VALUE;

        for (long value : array) {
            if (value < previous) {
                return false;
            }

            previous = value;
        }

        return true;
    }
}

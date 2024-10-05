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
package org.apache.pulsar.broker.service;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.SortedSet;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.Range;

/**
 * Represents the hash ranges impacted by a change in the hash range assignments.
 */
@EqualsAndHashCode
@ToString
public class ImpactedHashRanges {
    private final Range[] ranges;

    private ImpactedHashRanges(SortedSet<Range> ranges) {
        // Converts the set of ranges to an array to avoid iterator allocation
        // when the ranges are iterator multiple times in the pending acknowledgments loop.
        this.ranges = ranges.toArray(new Range[0]);
    }

    public static ImpactedHashRanges of(SortedSet<Range> ranges) {
        return new ImpactedHashRanges(ranges);
    }

    /**
     * Checks if the sticky key hash is contained in the impacted hash ranges.
     */
    public boolean containsStickyKey(int stickyKeyHash) {
        for (Range range : ranges) {
            if (range.contains(stickyKeyHash)) {
                return true;
            }
            // Since ranges are sorted, stop checking further ranges if the start of the current range is
            // greater than the stickyKeyHash.
            if (range.getStart() > stickyKeyHash) {
                return false;
            }
        }
        return false;
    }

    /**
     * Returns the ranges as an array. This method is only used for testing.
     */
    @VisibleForTesting
    public Range[] asRangeArray() {
        return Arrays.copyOf(ranges, ranges.length);
    }
}

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

import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.Range;

/**
 * Represents the hash ranges which were removed from an existing consumer by a change in the hash range assignments.
 */
@EqualsAndHashCode
@ToString
public class RemovedHashRanges {
    private final Range[] sortedRanges;

    private RemovedHashRanges(List<Range> ranges) {
        // Converts the set of ranges to an array to avoid iterator allocation
        // when the ranges are iterator multiple times in the pending acknowledgments loop.
        this.sortedRanges = ranges.toArray(new Range[0]);
        validateSortedRanges();
    }

    private void validateSortedRanges() {
        for (int i = 0; i < sortedRanges.length - 1; i++) {
            if (sortedRanges[i].getStart() >= sortedRanges[i + 1].getStart()) {
                throw new IllegalArgumentException(
                        "Ranges must be sorted: " + sortedRanges[i] + " and " + sortedRanges[i + 1]);
            }
            if (sortedRanges[i].getEnd() >= sortedRanges[i + 1].getStart()) {
                throw new IllegalArgumentException(
                        "Ranges must not overlap: " + sortedRanges[i] + " and " + sortedRanges[i + 1]);
            }
        }
    }

    public static RemovedHashRanges of(List<Range> ranges) {
        return new RemovedHashRanges(ranges);
    }

    /**
     * Checks if the sticky key hash is contained in the impacted hash ranges.
     */
    public boolean containsStickyKey(int stickyKeyHash) {
        for (Range range : sortedRanges) {
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
     * Checks if all removed ranges are fully contained in the provided list of ranges.
     */
    public boolean isFullyContainedInRanges(List<Range> otherRanges) {
        return Arrays.stream(sortedRanges).allMatch(range ->
                otherRanges.stream().anyMatch(otherRange -> otherRange.contains(range))
        );
    }

    /**
     * Returns the removed hash ranges as a list of ranges.
     */
    public List<Range> asRanges() {
        return Arrays.asList(sortedRanges);
    }
}

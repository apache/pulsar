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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.util.Murmur3_32Hash;

/**
 * Internal utility class for handling hash ranges.
 */
class HashRanges {
    /**
     * Resolve the impacted consumers where the existing range changed compared to the range before the change.
     * @param mappingBefore the range mapping before the change
     * @param mappingAfter the range mapping after the change
     * @return consumers and ranges where the existing range changed
     */
    static Map<Consumer, NavigableSet<Range>> resolveImpactedExistingConsumers(Map<Range, Consumer> mappingBefore,
                                                                               Map<Range, Consumer> mappingAfter) {
        Map<Range, Pair<Consumer, Consumer>> changedRanges = diffRanges(mappingBefore, mappingAfter);
        Map<Consumer, NavigableSet<Range>> impactedRangesByConsumer = changedRanges.entrySet().stream()
                .collect(HashMap::new, (map, entry) -> {
                    // consider only the consumers before the change
                    Consumer c = entry.getValue().getLeft();
                    if (c != null) {
                        Range range = entry.getKey();
                        map.computeIfAbsent(c, k -> new TreeSet<>()).add(range);
                    }
                }, HashMap::putAll);
        return impactedRangesByConsumer;
    }

    /**
     * Calculate the diff of two range mappings.
     * @param mappingBefore the range mapping before
     * @param mappingAfter the range mapping after
     * @return the impacted ranges where the consumer is changed from the before to the after
     */
    static Map<Range, Pair<Consumer, Consumer>> diffRanges(Map<Range, Consumer> mappingBefore,
                                                           Map<Range, Consumer> mappingAfter) {
        Map<Range, Pair<Consumer, Consumer>> impactedRanges = new LinkedHashMap<>();
        Iterator<Map.Entry<Range, Consumer>> beforeIterator = mappingBefore.entrySet().iterator();
        Iterator<Map.Entry<Range, Consumer>> afterIterator = mappingAfter.entrySet().iterator();

        Map.Entry<Range, Consumer> beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
        Map.Entry<Range, Consumer> afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;

        while (beforeEntry != null && afterEntry != null) {
            Range beforeRange = beforeEntry.getKey();
            Range afterRange = afterEntry.getKey();
            Consumer beforeConsumer = beforeEntry.getValue();
            Consumer afterConsumer = afterEntry.getValue();

            if (beforeRange.equals(afterRange)) {
                if (!beforeConsumer.equals(afterConsumer)) {
                    impactedRanges.put(afterRange, Pair.of(beforeConsumer, afterConsumer));
                }
                beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
                afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;
            } else if (beforeRange.getEnd() < afterRange.getStart()) {
                impactedRanges.put(beforeRange, Pair.of(beforeConsumer, afterConsumer));
                beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
            } else if (afterRange.getEnd() < beforeRange.getStart()) {
                impactedRanges.put(afterRange, Pair.of(beforeConsumer, afterConsumer));
                afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;
            } else {
                Range overlapRange = Range.of(
                        Math.max(beforeRange.getStart(), afterRange.getStart()),
                        Math.min(beforeRange.getEnd(), afterRange.getEnd())
                );
                if (!beforeConsumer.equals(afterConsumer)) {
                    impactedRanges.put(overlapRange, Pair.of(beforeConsumer, afterConsumer));
                }
                if (beforeRange.getEnd() <= overlapRange.getEnd()) {
                    beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
                }
                if (afterRange.getEnd() <= overlapRange.getEnd()) {
                    afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;
                }
            }
        }

        while (beforeEntry != null) {
            impactedRanges.put(beforeEntry.getKey(), Pair.of(beforeEntry.getValue(), null));
            beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
        }

        while (afterEntry != null) {
            impactedRanges.put(afterEntry.getKey(), Pair.of(null, afterEntry.getValue()));
            afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;
        }

        return impactedRanges;
    }

    public static int makeStickyKeyHash(byte[] stickyKey, int rangeSize) {
        int hashValue = Murmur3_32Hash.getInstance().makeHash(stickyKey) % rangeSize;
        // avoid using 0 as hash value since it is used as a special value in dispatchers
        if (hashValue == 0) {
            hashValue = 1;
        }
        return hashValue;
    }
}

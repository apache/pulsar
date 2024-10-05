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
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Range;
import org.jetbrains.annotations.NotNull;

/**
 * Represents the hash ranges assigned to each consumer in a {@link StickyKeyConsumerSelector} at a point in time.
 */
@EqualsAndHashCode(exclude = "cachedRangesByConsumer")
@ToString(exclude = "cachedRangesByConsumer")
public class ConsumerHashAssignmentsSnapshot {
    private final SortedMap<Range, Consumer> hashRangeAssignments;
    private Map<Consumer, List<Range>> cachedRangesByConsumer;

    private ConsumerHashAssignmentsSnapshot(SortedMap<Range, Consumer> hashRangeAssignments) {
        this.hashRangeAssignments = hashRangeAssignments;
    }

    public static ConsumerHashAssignmentsSnapshot of(SortedMap<Range, Consumer> hashRangeAssignments) {
        return new ConsumerHashAssignmentsSnapshot(hashRangeAssignments);
    }

    public static ConsumerHashAssignmentsSnapshot empty() {
        return new ConsumerHashAssignmentsSnapshot(Collections.emptySortedMap());
    }

    public ImpactedConsumersResult resolveImpactedConsumers(ConsumerHashAssignmentsSnapshot other) {
        return resolveConsumerRemovedHashRanges(this.hashRangeAssignments, other.hashRangeAssignments);
    }

    /**
     * Get the ranges assigned to each consumer. The ranges are merged if they are overlapping.
     * @return the ranges assigned to each consumer
     */
    public synchronized Map<Consumer, List<Range>> getRangesByConsumer() {
        if (cachedRangesByConsumer == null) {
            cachedRangesByConsumer = internalGetRangesByConsumer();
        }
        return cachedRangesByConsumer;
    }

    private @NotNull Map<Consumer, List<Range>> internalGetRangesByConsumer() {
        Map<Consumer, SortedSet<Range>> rangesByConsumer = new IdentityHashMap<>();
        hashRangeAssignments.forEach((range, consumer) -> {
            rangesByConsumer.computeIfAbsent(consumer, k -> new TreeSet<>()).add(range);
        });
        Map<Consumer, List<Range>> mergedOverlappingRangesByConsumer = new IdentityHashMap<>();
        rangesByConsumer.forEach((consumer, ranges) -> {
            mergedOverlappingRangesByConsumer.put(consumer, new ArrayList<>(mergeOverlappingRanges(ranges)));
        });
        return mergedOverlappingRangesByConsumer;
    }

    @VisibleForTesting
    Map<Range, Pair<Consumer, Consumer>> diffRanges(ConsumerHashAssignmentsSnapshot other) {
        return diffRanges(this.hashRangeAssignments, other.hashRangeAssignments);
    }

    /**
     * Resolve the consumers where the existing range was removed by a change.
     * @param mappingBefore the range mapping before the change
     * @param mappingAfter the range mapping after the change
     * @return consumers and ranges where the existing range changed
     */
    static ImpactedConsumersResult resolveConsumerRemovedHashRanges(SortedMap<Range, Consumer> mappingBefore,
                                                                    SortedMap<Range, Consumer> mappingAfter) {
        Map<Range, Pair<Consumer, Consumer>> changedRanges = diffRanges(mappingBefore, mappingAfter);
        Map<Consumer, SortedSet<Range>> removedRangesByConsumer = changedRanges.entrySet().stream()
                .collect(IdentityHashMap::new, (map, entry) -> {
                    Range range = entry.getKey();
                    Consumer consumerBefore = entry.getValue().getLeft();
                    addRange(map, consumerBefore, range);
                }, IdentityHashMap::putAll);
        return mergeOverlappingRanges(removedRangesByConsumer);
    }

    private static void addRange(Map<Consumer, SortedSet<Range>> map,
                                 Consumer c, Range range) {
        if (c != null) {
            map.computeIfAbsent(c, k -> new TreeSet<>()).add(range);
        }
    }

    static ImpactedConsumersResult mergeOverlappingRanges(
            Map<Consumer, SortedSet<Range>> removedRangesByConsumer) {
        Map<Consumer, RemovedHashRanges> mergedRangesByConsumer = new IdentityHashMap<>();
        removedRangesByConsumer.forEach((consumer, ranges) -> {
            mergedRangesByConsumer.put(consumer, RemovedHashRanges.of(mergeOverlappingRanges(ranges)));
        });
        return ImpactedConsumersResult.of(mergedRangesByConsumer);
    }

    static SortedSet<Range> mergeOverlappingRanges(SortedSet<Range> ranges) {
        TreeSet<Range> mergedRanges = new TreeSet<>();
        Iterator<Range> rangeIterator = ranges.iterator();
        Range currentRange = rangeIterator.hasNext() ? rangeIterator.next() : null;
        while (rangeIterator.hasNext()) {
            Range nextRange = rangeIterator.next();
            if (currentRange.getEnd() >= nextRange.getStart() - 1) {
                currentRange = Range.of(currentRange.getStart(), Math.max(currentRange.getEnd(), nextRange.getEnd()));
            } else {
                mergedRanges.add(currentRange);
                currentRange = nextRange;
            }
        }
        if (currentRange != null) {
            mergedRanges.add(currentRange);
        }
        return mergedRanges;
    }

    /**
     * Calculate the diff of two range mappings.
     * @param mappingBefore the range mapping before
     * @param mappingAfter the range mapping after
     * @return the impacted ranges where the consumer is changed from the before to the after
     */
    static Map<Range, Pair<Consumer, Consumer>> diffRanges(SortedMap<Range, Consumer> mappingBefore,
                                                           SortedMap<Range, Consumer> mappingAfter) {
        Map<Range, Pair<Consumer, Consumer>> removedRanges = new LinkedHashMap<>();
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
                    removedRanges.put(afterRange, Pair.of(beforeConsumer, afterConsumer));
                }
                beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
                afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;
            } else if (beforeRange.getEnd() < afterRange.getStart()) {
                removedRanges.put(beforeRange, Pair.of(beforeConsumer, afterConsumer));
                beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
            } else if (afterRange.getEnd() < beforeRange.getStart()) {
                removedRanges.put(afterRange, Pair.of(beforeConsumer, afterConsumer));
                afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;
            } else {
                Range overlapRange = Range.of(
                        Math.max(beforeRange.getStart(), afterRange.getStart()),
                        Math.min(beforeRange.getEnd(), afterRange.getEnd())
                );
                if (!beforeConsumer.equals(afterConsumer)) {
                    removedRanges.put(overlapRange, Pair.of(beforeConsumer, afterConsumer));
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
            removedRanges.put(beforeEntry.getKey(), Pair.of(beforeEntry.getValue(), null));
            beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
        }

        while (afterEntry != null) {
            removedRanges.put(afterEntry.getKey(), Pair.of(null, afterEntry.getValue()));
            afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;
        }

        return removedRanges;
    }
}

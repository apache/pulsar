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
    private final List<HashRangeAssignment> hashRangeAssignments;
    private Map<Consumer, List<Range>> cachedRangesByConsumer;

    private ConsumerHashAssignmentsSnapshot(List<HashRangeAssignment> hashRangeAssignments) {
        validate(hashRangeAssignments);
        this.hashRangeAssignments = hashRangeAssignments;
    }

    private void validate(List<HashRangeAssignment> hashRangeAssignments) {
        Range previousRange = null;
        for (HashRangeAssignment hashRangeAssignment : hashRangeAssignments) {
            Range range = hashRangeAssignment.range();
            Consumer consumer = hashRangeAssignment.consumer();
            if (range == null || consumer == null) {
                throw new IllegalArgumentException("Range and consumer must not be null");
            }
            if (previousRange != null && previousRange.compareTo(range) >= 0) {
                throw new IllegalArgumentException("Ranges must be non-overlapping and sorted");
            }
            previousRange = range;
        }
    }

    public static ConsumerHashAssignmentsSnapshot of(List<HashRangeAssignment> hashRangeAssignments) {
        return new ConsumerHashAssignmentsSnapshot(hashRangeAssignments);
    }

    public static ConsumerHashAssignmentsSnapshot empty() {
        return new ConsumerHashAssignmentsSnapshot(Collections.emptyList());
    }

    public ImpactedConsumersResult resolveImpactedConsumers(ConsumerHashAssignmentsSnapshot assignmentsAfter) {
        return resolveConsumerRemovedHashRanges(this.hashRangeAssignments, assignmentsAfter.hashRangeAssignments);
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
        hashRangeAssignments.forEach(entry -> {
            Range range = entry.range();
            Consumer consumer = entry.consumer();
            rangesByConsumer.computeIfAbsent(consumer, k -> new TreeSet<>()).add(range);
        });
        Map<Consumer, List<Range>> mergedOverlappingRangesByConsumer = new IdentityHashMap<>();
        rangesByConsumer.forEach((consumer, ranges) -> {
            mergedOverlappingRangesByConsumer.put(consumer, mergeOverlappingRanges(ranges));
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
    static ImpactedConsumersResult resolveConsumerRemovedHashRanges(List<HashRangeAssignment> mappingBefore,
                                                                    List<HashRangeAssignment> mappingAfter) {
        Map<Range, Pair<Consumer, Consumer>> impactedRanges = diffRanges(mappingBefore, mappingAfter);
        Map<Consumer, SortedSet<Range>> removedRangesByConsumer = impactedRanges.entrySet().stream()
                .collect(IdentityHashMap::new, (resultMap, entry) -> {
                    Range range = entry.getKey();
                    // filter out only where the range was removed
                    Consumer consumerBefore = entry.getValue().getLeft();
                    if (consumerBefore != null) {
                        resultMap.computeIfAbsent(consumerBefore, k -> new TreeSet<>()).add(range);
                    }
                }, IdentityHashMap::putAll);
        return mergedOverlappingRangesAndConvertToImpactedConsumersResult(removedRangesByConsumer);
    }

    static ImpactedConsumersResult mergedOverlappingRangesAndConvertToImpactedConsumersResult(
            Map<Consumer, SortedSet<Range>> removedRangesByConsumer) {
        Map<Consumer, RemovedHashRanges> mergedRangesByConsumer = new IdentityHashMap<>();
        removedRangesByConsumer.forEach((consumer, ranges) -> {
            mergedRangesByConsumer.put(consumer, RemovedHashRanges.of(mergeOverlappingRanges(ranges)));
        });
        return ImpactedConsumersResult.of(mergedRangesByConsumer);
    }

    /**
     * Merge overlapping ranges.
     * @param ranges the ranges to merge
     * @return the merged ranges
     */
    static List<Range> mergeOverlappingRanges(SortedSet<Range> ranges) {
        List<Range> mergedRanges = new ArrayList<>();
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
    static Map<Range, Pair<Consumer, Consumer>> diffRanges(List<HashRangeAssignment> mappingBefore,
                                                           List<HashRangeAssignment> mappingAfter) {
        Map<Range, Pair<Consumer, Consumer>> impactedRanges = new LinkedHashMap<>();
        Iterator<HashRangeAssignment> beforeIterator = mappingBefore.iterator();
        Iterator<HashRangeAssignment> afterIterator = mappingAfter.iterator();

        HashRangeAssignment beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
        HashRangeAssignment afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;

        while (beforeEntry != null && afterEntry != null) {
            Range beforeRange = beforeEntry.range();
            Range afterRange = afterEntry.range();
            Consumer beforeConsumer = beforeEntry.consumer();
            Consumer afterConsumer = afterEntry.consumer();

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
            impactedRanges.put(beforeEntry.range(), Pair.of(beforeEntry.consumer(), null));
            beforeEntry = beforeIterator.hasNext() ? beforeIterator.next() : null;
        }

        while (afterEntry != null) {
            impactedRanges.put(afterEntry.range(), Pair.of(null, afterEntry.consumer()));
            afterEntry = afterIterator.hasNext() ? afterIterator.next() : null;
        }

        return impactedRanges;
    }
}
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.IntRange;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * This is a sticky-key consumer selector based user provided range.
 * User is responsible for making sure provided range for all consumers cover the rangeSize
 * else there'll be chance that a key fall in a `whole` that not handled by any consumer.
 */
public class HashRangeExclusiveStickyKeyConsumerSelector implements StickyKeyConsumerSelector {

    private final int rangeSize;
    private final ConcurrentSkipListMap<Integer, Pair<Range, Consumer>> rangeMap;

    public HashRangeExclusiveStickyKeyConsumerSelector() {
        this(DEFAULT_RANGE_SIZE);
    }

    public HashRangeExclusiveStickyKeyConsumerSelector(int rangeSize) {
        super();
        if (rangeSize < 1) {
            throw new IllegalArgumentException("range size must greater than 0");
        }
        this.rangeSize = rangeSize;
        this.rangeMap = new ConcurrentSkipListMap<>();
    }

    @Override
    public synchronized CompletableFuture<Void> addConsumer(Consumer consumer) {
        return validateKeySharedMeta(consumer).thenRun(() -> {
            try {
                internalAddConsumer(consumer);
            } catch (BrokerServiceException.ConsumerAssignException e) {
                throw FutureUtil.wrapToCompletionException(e);
            }
        });
    }

    private synchronized void internalAddConsumer(Consumer consumer)
            throws BrokerServiceException.ConsumerAssignException {
        Consumer conflictingConsumer = findConflictingConsumer(consumer.getKeySharedMeta().getHashRangesList());
        if (conflictingConsumer != null) {
            throw new BrokerServiceException.ConsumerAssignException("Range conflict with consumer "
                    + conflictingConsumer);
        }
        for (IntRange intRange : consumer.getKeySharedMeta().getHashRangesList()) {
            rangeMap.put(intRange.getStart(), Pair.of(Range.of(intRange.getStart(), intRange.getEnd()), consumer));
        }
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        rangeMap.entrySet().removeIf(entry -> entry.getValue().getRight().equals(consumer));
    }

    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        Map<Consumer, List<Range>> result = new HashMap<>();
        for (Map.Entry<Integer, Pair<Range, Consumer>> entry : rangeMap.entrySet()) {
            Range assignedRange = entry.getValue().getLeft();
            Consumer assignedConsumer = entry.getValue().getRight();
            result.computeIfAbsent(assignedConsumer, key -> new ArrayList<>())
                    .add(assignedRange);
        }
        return result;
    }

    @Override
    public Consumer select(int hash) {
        if (rangeMap.isEmpty()) {
            return null;
        }

        int slot = hash % rangeSize;
        Map.Entry<Integer, Pair<Range, Consumer>> floorEntry = rangeMap.floorEntry(slot);
        if (floorEntry == null) {
            return null;
        }
        Pair<Range, Consumer> pair = floorEntry.getValue();
        if (pair.getLeft().contains(slot)) {
            return pair.getRight();
        } else {
            return null;
        }
    }

    private synchronized CompletableFuture<Void> validateKeySharedMeta(Consumer consumer) {
        if (consumer.getKeySharedMeta() == null) {
            return FutureUtil.failedFuture(
                    new BrokerServiceException.ConsumerAssignException("Must specify key shared meta for consumer."));
        }
        List<IntRange> ranges = consumer.getKeySharedMeta().getHashRangesList();
        if (ranges.isEmpty()) {
            return FutureUtil.failedFuture(new BrokerServiceException.ConsumerAssignException(
                    "Ranges for KeyShared policy must not be empty."));
        }
        List<IntRange> sortedRanges = new ArrayList<>(ranges);
        sortedRanges.sort(Comparator.comparingInt(IntRange::getStart));
        for (int i = 0; i < sortedRanges.size(); i++) {
            IntRange currentRange = sortedRanges.get(i);
            // 1. Validate: check if start > end for the current range
            if (currentRange.getStart() > currentRange.getEnd()) {
                return FutureUtil.failedFuture(
                        new BrokerServiceException.ConsumerAssignException("Fixed hash range start > end for range: "
                                + "[" + currentRange.getStart() + "," + currentRange.getEnd() + "]"));
            }
            // 2. Validate: check for overlaps with the next range in the sorted list
            if (i < sortedRanges.size() - 1) {
                IntRange nextRange = sortedRanges.get(i + 1);
                if (areRangesOverlapping(currentRange, nextRange)) {
                    return FutureUtil.failedFuture(
                            new BrokerServiceException.ConsumerAssignException("Consumer's own ranges conflict: "
                                    + "[" + currentRange.getStart() + "," + currentRange.getEnd() + "] "
                                    + "overlaps with [" + nextRange.getStart() + "," + nextRange.getEnd() + "]"));
                }
            }
        }
        Consumer conflictingConsumer = findConflictingConsumer(ranges);
        if (conflictingConsumer != null) {
            return conflictingConsumer.cnx().checkConnectionLiveness().thenRun(() -> {});
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private synchronized Consumer findConflictingConsumer(List<IntRange> newConsumerRanges) {
        for (IntRange newRange : newConsumerRanges) {
            // 1. Check for potential conflicts with existing ranges that start before newRange's start.
            Map.Entry<Integer, Pair<Range, Consumer>> conflictBeforeStart = rangeMap.floorEntry(newRange.getStart());
            if (conflictBeforeStart != null) {
                Range existingRange = conflictBeforeStart.getValue().getLeft();
                if (areRangesOverlapping(newRange, existingRange)) {
                    return conflictBeforeStart.getValue().getRight();
                }
            }
            // 2. Check for potential conflicts with existing ranges that start after newRange's start.
            Map.Entry<Integer, Pair<Range, Consumer>> conflictAfterStart = rangeMap.ceilingEntry(newRange.getStart());
            if (conflictAfterStart != null) {
                Range existingRange = conflictAfterStart.getValue().getLeft();
                if (areRangesOverlapping(newRange, existingRange)) {
                    return conflictAfterStart.getValue().getRight();
                }
            }
        }
        return null;
    }


    private static boolean areRangesOverlapping(IntRange range1, Range range2) {
        return Math.max(range1.getStart(), range2.getStart()) <= Math.min(range1.getEnd(), range2.getEnd());
    }

    private static boolean areRangesOverlapping(IntRange range1, IntRange range2) {
        return Math.max(range1.getStart(), range2.getStart()) <= Math.min(range1.getEnd(), range2.getEnd());
    }

    Map<Integer, Pair<Range, Consumer>> getRangeConsumer() {
        return Collections.unmodifiableMap(rangeMap);
    }

}

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Range;

/**
 * This is a consumer selector using consistent hashing to evenly split
 * the number of keys assigned to each consumer.
 *
 * <p>The hash ring is kept in two arrays, the sorted hash ring points and the consumer selected at each point, which
 * are replaced, never modified, on every membership change. The arrays serve the lookups and, compared with the
 * previous arrays, also give the hash ranges that a membership change moved between consumers. A Key_Shared
 * subscription with many consumers costs a few bytes per hash ring point, and the selector's work on a membership
 * change is a few passes over the arrays, which matters when many consumers reconnect at the same time.
 */
@CustomLog
public class ConsistentHashingStickyKeyConsumerSelector implements StickyKeyConsumerSelector {
    // use NUL character as field separator for hash key calculation
    private static final String KEY_SEPARATOR = "\0";
    // serializes the membership changes; the lookups read the published ring without locking
    private final Lock membershipLock = new ReentrantLock();

    /**
     * The hash ring: the sorted hash ring points and the consumer selected at each point. The arrays are never
     * modified after publication, so readers capture one ring and its points and consumers always belong to the same
     * completed membership change.
     */
    private record HashRing(int[] points, Consumer[] consumers) {
        static final HashRing EMPTY = new HashRing(new int[0], new Consumer[0]);

        boolean isEmpty() {
            return points.length == 0;
        }

        /** The consumer of the first point at or after the given index, wrapping around, or null for an empty ring. */
        Consumer consumerAt(int index) {
            if (points.length == 0) {
                return null;
            }
            return consumers[index < points.length ? index : 0];
        }
    }

    private volatile HashRing ring = HashRing.EMPTY;
    // Consumers whose hash ring point collided with the point of the selected consumer, by the point. The first
    // colliding consumer is selected when the selected consumer is removed from the hash ring. Collisions are rare,
    // so the map is created when the first one happens.
    private Map<Integer, List<Consumer>> collidingConsumers;
    // Tracks the used consumer name indexes for each consumer name
    private final ConsumerNameIndexTracker consumerNameIndexTracker = new ConsumerNameIndexTracker();
    // The sorted hash ring points of each consumer, by its identity, so that removing a consumer doesn't calculate
    // them again
    private final Map<Consumer, int[]> consumerPoints = new IdentityHashMap<>();

    private final int numberOfPoints;
    private final Range keyHashRange;
    private final boolean addOrRemoveReturnsImpactedConsumersResult;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this(numberOfPoints, false);
    }

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints,
                                                      boolean addOrRemoveReturnsImpactedConsumersResult) {
        this(numberOfPoints, addOrRemoveReturnsImpactedConsumersResult, DEFAULT_RANGE_SIZE - 1);
    }

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints,
                                                      boolean addOrRemoveReturnsImpactedConsumersResult,
                                                      int rangeMaxValue) {
        this.addOrRemoveReturnsImpactedConsumersResult = addOrRemoveReturnsImpactedConsumersResult;
        this.numberOfPoints = numberOfPoints;
        this.keyHashRange = Range.of(STICKY_KEY_HASH_NOT_SET + 1, rangeMaxValue);
    }

    @Override
    public CompletableFuture<Optional<ImpactedConsumersResult>> addConsumer(Consumer consumer) {
        membershipLock.lock();
        try {
            HashRing before = ring;
            ConsumerIdentityWrapper consumerIdentityWrapper = new ConsumerIdentityWrapper(consumer);
            // Insert multiple points on the hash ring for every consumer
            // The points are deterministically added based on the hash of the consumer name
            int[] points = new int[numberOfPoints];
            int[] addedPoints = new int[numberOfPoints];
            int addedPointCount = 0;
            int hashPointCollisions = 0;
            for (int i = 0; i < numberOfPoints; i++) {
                int consumerNameIndex =
                        consumerNameIndexTracker.increaseConsumerRefCountAndReturnIndex(consumerIdentityWrapper);
                int hash = calculateHashForConsumerAndIndex(consumer, consumerNameIndex, i);
                points[i] = hash;
                // When there's a collision, the entry won't be selected in the hash ring.
                // This isn't a problem with the consumerNameIndexTracker solution since the collisions won't align
                // for all hash ring points when using the same consumer name. This won't affect the overall
                // distribution significantly when the number of hash ring points is sufficiently large (>100).
                if (Arrays.binarySearch(before.points(), hash) >= 0) {
                    hashPointCollisions++;
                    addCollidingConsumer(hash, consumer);
                } else {
                    addedPoints[addedPointCount++] = hash;
                }
            }
            if (numberOfPoints > 0) {
                Arrays.sort(points);
                consumerPoints.put(consumer, points);
            }
            Arrays.sort(addedPoints, 0, addedPointCount);
            // the consumer's own points can collide with each other too
            int hashPointsAdded = 0;
            for (int i = 0; i < addedPointCount; i++) {
                if (hashPointsAdded > 0 && addedPoints[hashPointsAdded - 1] == addedPoints[i]) {
                    hashPointCollisions++;
                    addCollidingConsumer(addedPoints[i], consumer);
                } else {
                    addedPoints[hashPointsAdded++] = addedPoints[i];
                }
            }
            if (hashPointsAdded == 0) {
                log.error()
                        .attr("consumer", consumer)
                        .attr("hashPointCollisions", hashPointCollisions)
                        .attr("numberOfPoints", numberOfPoints)
                        .attr("suggestedReplicaPoints",
                                Math.max((int) (numberOfPoints * 1.5d), numberOfPoints + 1))
                        .log("Failed to add consumer to the hash ring due to collisions. Consider increasing "
                                + "subscriptionKeySharedConsistentHashingReplicaPoints");
            }
            log.debug()
                    .attr("consumer", consumer)
                    .attr("hashPointsAdded", hashPointsAdded)
                    .attr("hashPointCollisions", hashPointCollisions)
                    .log("Added consumer");
            HashRing after = hashPointsAdded > 0 ? merge(before, addedPoints, hashPointsAdded, consumer) : before;
            ring = after;
            if (!addOrRemoveReturnsImpactedConsumersResult) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return CompletableFuture.completedFuture(Optional.of(resolveImpactedConsumers(before, after)));
        } finally {
            membershipLock.unlock();
        }
    }

    private void addCollidingConsumer(int hash, Consumer consumer) {
        if (collidingConsumers == null) {
            collidingConsumers = new HashMap<>();
        }
        collidingConsumers.computeIfAbsent(hash, k -> new ArrayList<>(1)).add(consumer);
    }

    /** A new ring with the given sorted points, which aren't in the ring, selecting the given consumer. */
    private static HashRing merge(HashRing before, int[] addedPoints, int addedPointCount, Consumer consumer) {
        int[] beforePoints = before.points();
        Consumer[] beforeConsumers = before.consumers();
        int size = beforePoints.length + addedPointCount;
        int[] points = new int[size];
        Consumer[] consumers = new Consumer[size];
        int beforeIndex = 0;
        int addedIndex = 0;
        for (int i = 0; i < size; i++) {
            if (addedIndex == addedPointCount
                    || (beforeIndex < beforePoints.length && beforePoints[beforeIndex] < addedPoints[addedIndex])) {
                points[i] = beforePoints[beforeIndex];
                consumers[i] = beforeConsumers[beforeIndex++];
            } else {
                points[i] = addedPoints[addedIndex++];
                consumers[i] = consumer;
            }
        }
        return new HashRing(points, consumers);
    }

    /**
     * Calculate the hash for a consumer and hash ring point.
     * The hash is calculated based on the consumer name, consumer name index, and hash ring point index.
     * The resulting hash is used as the key to insert the consumer into the hash ring.
     *
     * @param consumer the consumer
     * @param consumerNameIndex the index of the consumer name
     * @param hashRingPointIndex the index of the hash ring point
     * @return the hash value
     */
    private int calculateHashForConsumerAndIndex(Consumer consumer, int consumerNameIndex,
                                                        int hashRingPointIndex) {
        String key = consumer.consumerName() + KEY_SEPARATOR + consumerNameIndex + KEY_SEPARATOR + hashRingPointIndex;
        return makeStickyKeyHash(key.getBytes());
    }

    @Override
    public Optional<ImpactedConsumersResult> removeConsumer(Consumer consumer) {
        membershipLock.lock();
        try {
            HashRing before = ring;
            HashRing after = before;
            ConsumerIdentityWrapper consumerIdentityWrapper = new ConsumerIdentityWrapper(consumer);
            int consumerNameIndex = consumerNameIndexTracker.getTrackedIndex(consumerIdentityWrapper);
            if (consumerNameIndex > -1) {
                int[] points = consumerPoints.remove(consumer);
                if (points == null) {
                    points = new int[numberOfPoints];
                    for (int i = 0; i < numberOfPoints; i++) {
                        points[i] = calculateHashForConsumerAndIndex(consumer, consumerNameIndex, i);
                    }
                    Arrays.sort(points);
                }
                // The consumer's points in the ring and each point's consumer after removing this consumer. The
                // points are sorted, so a point that the consumer has more than once comes in a row.
                int[] changedIndexes = new int[points.length];
                Consumer[] changedConsumers = new Consumer[points.length];
                int changes = 0;
                int removedPoints = 0;
                for (int hash : points) {
                    int index = Arrays.binarySearch(before.points(), hash);
                    assert index >= 0 : "hash ring entry wasn't found for hash " + hash;
                    if (index < 0) {
                        continue;
                    }
                    if (changes > 0 && changedIndexes[changes - 1] == index) {
                        Consumer selected = changedConsumers[changes - 1];
                        if (selected != null) {
                            changedConsumers[changes - 1] = removeConsumerFromPoint(hash, selected, consumer);
                            removedPoints += changedConsumers[changes - 1] == null ? 1 : 0;
                        }
                    } else {
                        changedIndexes[changes] = index;
                        changedConsumers[changes] = removeConsumerFromPoint(hash, before.consumers()[index], consumer);
                        removedPoints += changedConsumers[changes] == null ? 1 : 0;
                        changes++;
                    }
                }
                for (int i = 0; i < numberOfPoints; i++) {
                    consumerNameIndexTracker.decreaseConsumerRefCount(consumerIdentityWrapper);
                }
                if (consumerNameIndexTracker.getTrackedIndex(consumerIdentityWrapper) > -1) {
                    // the consumer was added more than once, and is still in the ring
                    consumerPoints.put(consumer, points);
                }
                after = changes > 0 ? applyChanges(before, changedIndexes, changedConsumers, changes, removedPoints)
                        : before;
                ring = after;
            }
            if (!addOrRemoveReturnsImpactedConsumersResult) {
                return Optional.empty();
            }
            return Optional.of(resolveImpactedConsumers(before, after));
        } finally {
            membershipLock.unlock();
        }
    }

    /**
     * Remove a consumer from a hash ring point. When the selected consumer is removed, the first colliding consumer
     * is selected as the new selected consumer and removed from the colliding consumers.
     *
     * @return the point's selected consumer after the removal, or null when the point should be removed
     */
    private Consumer removeConsumerFromPoint(int hash, Consumer selectedConsumer, Consumer consumer) {
        List<Consumer> colliding = collidingConsumers != null ? collidingConsumers.get(hash) : null;
        if (selectedConsumer == consumer) {
            if (colliding == null) {
                return null;
            }
            selectedConsumer = colliding.remove(0);
        } else if (colliding != null) {
            // remove using identity comparison
            colliding.removeIf(c -> c == consumer);
        }
        if (colliding != null && colliding.isEmpty()) {
            collidingConsumers.remove(hash);
        }
        return selectedConsumer;
    }

    /**
     * A new ring with the given points' consumers changed, in one pass, leaving out the points whose consumer became
     * null.
     */
    private static HashRing applyChanges(HashRing before, int[] changedIndexes, Consumer[] changedConsumers,
                                         int changes, int removedPoints) {
        int[] beforePoints = before.points();
        Consumer[] beforeConsumers = before.consumers();
        int[] points = removedPoints > 0 ? new int[beforePoints.length - removedPoints] : beforePoints;
        Consumer[] consumers = new Consumer[points.length];
        int index = 0;
        int change = 0;
        for (int i = 0; i < beforePoints.length; i++) {
            Consumer consumer = beforeConsumers[i];
            if (change < changes && changedIndexes[change] == i) {
                consumer = changedConsumers[change++];
                if (consumer == null) {
                    continue;
                }
            }
            points[index] = beforePoints[i];
            consumers[index++] = consumer;
        }
        return new HashRing(points, consumers);
    }

    /**
     * Resolve the hash ranges that a membership change moved from one consumer to another, by walking the points of
     * both rings: a hash belongs to the consumer of the first point at or after it.
     */
    private ImpactedConsumersResult resolveImpactedConsumers(HashRing before, HashRing after) {
        if (before == after) {
            return ImpactedConsumersResult.of(Map.of(), Map.of());
        }
        Map<Consumer, List<Range>> removedRanges = new IdentityHashMap<>();
        Map<Consumer, List<Range>> addedRanges = new IdentityHashMap<>();
        int[] beforePoints = before.points();
        int[] afterPoints = after.points();
        int beforeIndex = 0;
        int afterIndex = 0;
        int start = keyHashRange.getStart();
        int end = keyHashRange.getEnd();
        while (true) {
            int beforeNext = beforeIndex < beforePoints.length ? beforePoints[beforeIndex] : end;
            int afterNext = afterIndex < afterPoints.length ? afterPoints[afterIndex] : end;
            int segmentEnd = Math.min(beforeNext, afterNext);
            Consumer consumerBefore = before.consumerAt(beforeIndex);
            Consumer consumerAfter = after.consumerAt(afterIndex);
            if (consumerBefore != consumerAfter && (consumerBefore == null || !consumerBefore.equals(consumerAfter))) {
                if (consumerBefore != null) {
                    appendRange(removedRanges, consumerBefore, start, segmentEnd);
                }
                if (consumerAfter != null) {
                    appendRange(addedRanges, consumerAfter, start, segmentEnd);
                }
            }
            if (segmentEnd >= end) {
                break;
            }
            if (beforeIndex < beforePoints.length && beforeNext == segmentEnd) {
                beforeIndex++;
            }
            if (afterIndex < afterPoints.length && afterNext == segmentEnd) {
                afterIndex++;
            }
            start = segmentEnd + 1;
        }
        return ImpactedConsumersResult.of(toUpdatedHashRanges(removedRanges), toUpdatedHashRanges(addedRanges));
    }

    private static void appendRange(Map<Consumer, List<Range>> rangesByConsumer, Consumer consumer, int start,
                                    int end) {
        List<Range> ranges = rangesByConsumer.computeIfAbsent(consumer, k -> new ArrayList<>());
        int last = ranges.size() - 1;
        if (last >= 0 && ranges.get(last).getEnd() >= start - 1) {
            ranges.set(last, Range.of(ranges.get(last).getStart(), end));
        } else {
            ranges.add(Range.of(start, end));
        }
    }

    private static Map<Consumer, UpdatedHashRanges> toUpdatedHashRanges(Map<Consumer, List<Range>> rangesByConsumer) {
        Map<Consumer, UpdatedHashRanges> result = new IdentityHashMap<>(rangesByConsumer.size());
        rangesByConsumer.forEach((consumer, ranges) -> result.put(consumer, UpdatedHashRanges.of(ranges)));
        return result;
    }

    @VisibleForTesting
    int getConsumerPointsCount() {
        membershipLock.lock();
        try {
            return consumerPoints.size();
        } finally {
            membershipLock.unlock();
        }
    }

    @Override
    public Consumer select(int hash) {
        HashRing current = ring;
        int[] points = current.points();
        if (points.length == 0) {
            return null;
        }
        int index = Arrays.binarySearch(points, hash);
        if (index < 0) {
            index = -index - 1;
        }
        return current.consumerAt(index);
    }

    @Override
    public Range getKeyHashRange() {
        return keyHashRange;
    }

    /**
     * Get the hash ranges of each consumer, built from the hash ring in one pass. The ring's consecutive points of a
     * consumer are joined into one range, so each consumer's ranges are sorted and don't touch each other.
     */
    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        HashRing current = ring;
        if (current.isEmpty()) {
            return Map.of();
        }
        int[] points = current.points();
        Consumer[] consumers = current.consumers();
        Map<Consumer, List<Range>> rangesByConsumer = new IdentityHashMap<>();
        // a long, since the range can end at Integer.MAX_VALUE
        long start = keyHashRange.getStart();
        int index = 0;
        while (index < points.length) {
            Consumer consumer = consumers[index];
            // join the consumer's consecutive points
            while (index + 1 < points.length && consumers[index + 1] == consumer) {
                index++;
            }
            int end = points[index];
            if (index == points.length - 1 && consumer == consumers[0] && end != keyHashRange.getEnd()) {
                // the last range wraps around to the first point's consumer
                end = keyHashRange.getEnd();
            }
            rangesByConsumer.computeIfAbsent(consumer, k -> new ArrayList<>()).add(Range.of((int) start, end));
            start = end + 1L;
            index++;
        }
        if (start <= keyHashRange.getEnd()) {
            // the hashes after the last point belong to the first point's consumer
            rangesByConsumer.computeIfAbsent(consumers[0], k -> new ArrayList<>())
                    .add(Range.of((int) start, keyHashRange.getEnd()));
        }
        return rangesByConsumer;
    }

    /**
     * Get the current hash range assignments. They are built from the hash ring on each call, since the selector
     * doesn't keep them.
     */
    @Override
    public ConsumerHashAssignmentsSnapshot getConsumerHashAssignmentsSnapshot() {
        HashRing current = ring;
        if (current.isEmpty()) {
            return ConsumerHashAssignmentsSnapshot.empty();
        }
        int[] points = current.points();
        Consumer[] consumers = current.consumers();
        List<HashRangeAssignment> result = new ArrayList<>();
        // a long, since the range can end at Integer.MAX_VALUE
        long start = keyHashRange.getStart();
        for (int i = 0; i < points.length; i++) {
            appendAssignment(result, (int) start, points[i], consumers[i]);
            start = points[i] + 1L;
        }
        // Handle wrap-around
        if (start <= keyHashRange.getEnd()) {
            appendAssignment(result, (int) start, keyHashRange.getEnd(), consumers[0]);
        }
        return ConsumerHashAssignmentsSnapshot.of(result);
    }

    private static void appendAssignment(List<HashRangeAssignment> assignments, int start, int end,
                                         Consumer consumer) {
        int last = assignments.size() - 1;
        if (last >= 0 && assignments.get(last).consumer() == consumer) {
            // join ranges
            Range previousRange = assignments.get(last).range();
            assignments.set(last, new HashRangeAssignment(Range.of(previousRange.getStart(), end), consumer));
        } else {
            assignments.add(new HashRangeAssignment(Range.of(start, end), consumer));
        }
    }
}

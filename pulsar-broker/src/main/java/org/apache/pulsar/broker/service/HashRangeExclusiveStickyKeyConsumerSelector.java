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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.api.proto.IntRange;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * This is a sticky-key consumer selector based user provided range.
 * User is responsible for making sure provided range for all consumers cover the rangeSize
 * else there'll be chance that a key fall in a `whole` that not handled by any consumer.
 */
public class HashRangeExclusiveStickyKeyConsumerSelector implements StickyKeyConsumerSelector {

    private final int rangeSize;
    private final ConcurrentSkipListMap<Integer, Consumer> rangeMap;
    private ConsumerHashAssignmentsSnapshot consumerHashAssignmentsSnapshot;

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
        this.consumerHashAssignmentsSnapshot = ConsumerHashAssignmentsSnapshot.empty();
    }

    @Override
    public synchronized CompletableFuture<Map<Consumer, ImpactedHashRanges>> addConsumer(Consumer consumer) {
        return validateKeySharedMeta(consumer).thenApply(__ -> {
            try {
                return internalAddConsumer(consumer);
            } catch (BrokerServiceException.ConsumerAssignException e) {
                throw FutureUtil.wrapToCompletionException(e);
            }
        });
    }

    private synchronized Map<Consumer, ImpactedHashRanges> internalAddConsumer(Consumer consumer)
            throws BrokerServiceException.ConsumerAssignException {
        Consumer conflictingConsumer = findConflictingConsumer(consumer.getKeySharedMeta().getHashRangesList());
        if (conflictingConsumer != null) {
            throw new BrokerServiceException.ConsumerAssignException("Range conflict with consumer "
                    + conflictingConsumer);
        }
        for (IntRange intRange : consumer.getKeySharedMeta().getHashRangesList()) {
            rangeMap.put(intRange.getStart(), consumer);
            rangeMap.put(intRange.getEnd(), consumer);
        }
        ConsumerHashAssignmentsSnapshot assignmentsAfter = internalGetConsumerHashAssignmentsSnapshot();
        Map<Consumer, ImpactedHashRanges> impactedRanges =
                consumerHashAssignmentsSnapshot.resolveImpactedHashRanges(assignmentsAfter);
        consumerHashAssignmentsSnapshot = assignmentsAfter;
        return impactedRanges;
    }

    @Override
    public synchronized Map<Consumer, ImpactedHashRanges> removeConsumer(Consumer consumer) {
        rangeMap.entrySet().removeIf(entry -> entry.getValue().equals(consumer));
        ConsumerHashAssignmentsSnapshot assignmentsAfter = internalGetConsumerHashAssignmentsSnapshot();
        Map<Consumer, ImpactedHashRanges> impactedRanges =
                consumerHashAssignmentsSnapshot.resolveImpactedHashRanges(assignmentsAfter);
        consumerHashAssignmentsSnapshot = assignmentsAfter;
        return impactedRanges;
    }

    @Override
    public synchronized ConsumerHashAssignmentsSnapshot getConsumerHashAssignmentsSnapshot() {
        return consumerHashAssignmentsSnapshot;
    }

    private ConsumerHashAssignmentsSnapshot internalGetConsumerHashAssignmentsSnapshot() {
        SortedMap<Range, Consumer> result = new TreeMap<>();
        Map.Entry<Integer, Consumer> prev = null;
        for (Map.Entry<Integer, Consumer> entry: rangeMap.entrySet()) {
            if (prev == null) {
                prev = entry;
            } else {
                if (prev.getValue().equals(entry.getValue())) {
                    result.put(Range.of(prev.getKey(), entry.getKey()), entry.getValue());
                }
                prev = null;
            }
        }
        return ConsumerHashAssignmentsSnapshot.of(result);
    }

    @Override
    public int makeStickyKeyHash(byte[] stickyKey) {
        return HashRanges.makeStickyKeyHash(stickyKey, rangeSize);
    }

    @Override
    public Consumer select(int hash) {
        if (rangeMap.size() > 0) {
            Map.Entry<Integer, Consumer> ceilingEntry = rangeMap.ceilingEntry(hash);
            Map.Entry<Integer, Consumer> floorEntry = rangeMap.floorEntry(hash);
            Consumer ceilingConsumer = ceilingEntry != null ? ceilingEntry.getValue() : null;
            Consumer floorConsumer = floorEntry != null ? floorEntry.getValue() : null;
            if (floorConsumer != null && floorConsumer.equals(ceilingConsumer)) {
                return ceilingConsumer;
            } else {
                return null;
            }
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
        for (IntRange intRange : ranges) {
            if (intRange.getStart() > intRange.getEnd()) {
                return FutureUtil.failedFuture(
                        new BrokerServiceException.ConsumerAssignException("Fixed hash range start > end"));
            }
        }
        Consumer conflictingConsumer = findConflictingConsumer(ranges);
        if (conflictingConsumer != null) {
            return conflictingConsumer.cnx().checkConnectionLiveness().thenRun(() -> {});
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private synchronized Consumer findConflictingConsumer(List<IntRange> ranges) {
        for (IntRange intRange : ranges) {
            Map.Entry<Integer, Consumer> ceilingEntry = rangeMap.ceilingEntry(intRange.getStart());
            Map.Entry<Integer, Consumer> floorEntry = rangeMap.floorEntry(intRange.getEnd());

            if (floorEntry != null && floorEntry.getKey() >= intRange.getStart()) {
                return floorEntry.getValue();
            }

            if (ceilingEntry != null && ceilingEntry.getKey() <= intRange.getEnd()) {
                return ceilingEntry.getValue();
            }

            if (ceilingEntry != null && floorEntry != null && ceilingEntry.getValue().equals(floorEntry.getValue())) {
                KeySharedMeta keySharedMeta = ceilingEntry.getValue().getKeySharedMeta();
                for (IntRange range : keySharedMeta.getHashRangesList()) {
                    int start = Math.max(intRange.getStart(), range.getStart());
                    int end = Math.min(intRange.getEnd(), range.getEnd());
                    if (end >= start) {
                        return ceilingEntry.getValue();
                    }
                }
            }
        }
        return null;
    }

    Map<Integer, Consumer> getRangeConsumer() {
        return Collections.unmodifiableMap(rangeMap);
    }

    @Override
    public Range getKeyHashRange() {
        return Range.of(0, rangeSize - 1);
    }
}

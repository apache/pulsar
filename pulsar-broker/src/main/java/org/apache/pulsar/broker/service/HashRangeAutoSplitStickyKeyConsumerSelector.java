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
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;

/**
 * This is a consumer selector based fixed hash range.
 *
 * 1.Each consumer serves a fixed range of hash value
 * 2.The whole range of hash value could be covered by all the consumers.
 * 3.Once a consumer is removed, the left consumers could still serve the whole range.
 *
 * Initializing with a fixed hash range, by default 2 &gt;&gt; 15.
 * First consumer added, hash range looks like:
 *
 * 0 -&lt; 65536(consumer-1)
 *
 * Second consumer added, will find a biggest range to split:
 *
 * 0 -&lt; 32768(consumer-2) -&lt; 65536(consumer-1)
 *
 * While a consumer removed, The range for this consumer will be taken over
 * by other consumer, consumer-2 removed:
 *
 * 0 -&lt; 65536(consumer-1)
 *
 * In this approach use skip list map to maintain the hash range and consumers.
 *
 * Select consumer will return the ceiling key of message key hashcode % range size.
 *
 */
public class HashRangeAutoSplitStickyKeyConsumerSelector implements StickyKeyConsumerSelector {

    private final int rangeSize;

    private final ConcurrentSkipListMap<Integer, Consumer> rangeMap;
    private final Map<Consumer, Integer> consumerRange;

    public HashRangeAutoSplitStickyKeyConsumerSelector() {
        this(DEFAULT_RANGE_SIZE);
    }

    public HashRangeAutoSplitStickyKeyConsumerSelector(int rangeSize) {
        if (rangeSize < 2) {
            throw new IllegalArgumentException("range size must greater than 2");
        }
        if (!is2Power(rangeSize)) {
            throw new IllegalArgumentException("range size must be nth power with 2");
        }
        this.rangeMap = new ConcurrentSkipListMap<>();
        this.consumerRange = new HashMap<>();
        this.rangeSize = rangeSize;
    }

    @Override
    public synchronized CompletableFuture<Map<Consumer, ImpactedHashRanges>> addConsumer(Consumer consumer) {
        ConsumerHashAssignmentsSnapshot assignmentsBefore = getConsumerHashAssignmentsSnapshot();
        if (rangeMap.isEmpty()) {
            rangeMap.put(rangeSize, consumer);
            consumerRange.put(consumer, rangeSize);
        } else {
            try {
                splitRange(findBiggestRange(), consumer);
            } catch (ConsumerAssignException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        ConsumerHashAssignmentsSnapshot assignmentsAfter = getConsumerHashAssignmentsSnapshot();
        Map<Consumer, ImpactedHashRanges> impactedRanges =
                assignmentsBefore.resolveImpactedHashRanges(assignmentsAfter);
        return CompletableFuture.completedFuture(impactedRanges);
    }

    @Override
    public synchronized Map<Consumer, ImpactedHashRanges> removeConsumer(Consumer consumer) {
        ConsumerHashAssignmentsSnapshot assignmentsBefore = getConsumerHashAssignmentsSnapshot();
        Integer removeRange = consumerRange.remove(consumer);
        if (removeRange != null) {
            if (removeRange == rangeSize && rangeMap.size() > 1) {
                Map.Entry<Integer, Consumer> lowerEntry = rangeMap.lowerEntry(removeRange);
                rangeMap.put(removeRange, lowerEntry.getValue());
                rangeMap.remove(lowerEntry.getKey());
                consumerRange.put(lowerEntry.getValue(), removeRange);
            } else {
                rangeMap.remove(removeRange);
            }
        }
        ConsumerHashAssignmentsSnapshot assignmentsAfter = getConsumerHashAssignmentsSnapshot();
        Map<Consumer, ImpactedHashRanges> impactedRanges =
                assignmentsBefore.resolveImpactedHashRanges(assignmentsAfter);
        return impactedRanges;
    }

    @Override
    public int makeStickyKeyHash(byte[] stickyKey) {
        return HashRanges.makeStickyKeyHash(stickyKey, rangeSize);
    }

    @Override
    public Consumer select(int hash) {
        if (!rangeMap.isEmpty()) {
            return rangeMap.ceilingEntry(hash).getValue();
        } else {
            return null;
        }
    }

    @Override
    public Range getKeyHashRange() {
        return Range.of(0, rangeSize - 1);
    }

    @Override
    public synchronized ConsumerHashAssignmentsSnapshot getConsumerHashAssignmentsSnapshot() {
        SortedMap<Range, Consumer> result = new TreeMap<>();
        int start = 0;
        for (Map.Entry<Integer, Consumer> entry: rangeMap.entrySet()) {
            result.put(Range.of(start, entry.getKey()), entry.getValue());
            start = entry.getKey() + 1;
        }
        return ConsumerHashAssignmentsSnapshot.of(result);
    }

    private int findBiggestRange() {
        int slots = 0;
        int busiestRange = rangeSize;
        for (Entry<Integer, Consumer> entry : rangeMap.entrySet()) {
            Integer lowerKey = rangeMap.lowerKey(entry.getKey());
            if (lowerKey == null) {
                lowerKey = 0;
            }
            if (entry.getKey() - lowerKey > slots) {
                slots = entry.getKey() - lowerKey;
                busiestRange = entry.getKey();
            }
        }
        return busiestRange;
    }

    private void splitRange(int range, Consumer targetConsumer) throws ConsumerAssignException {
        Integer lowerKey = rangeMap.lowerKey(range);
        if (lowerKey == null) {
            lowerKey = 0;
        }
        if (range - lowerKey <= 1) {
            throw new ConsumerAssignException("No more range can assigned to new consumer, assigned consumers "
                    + rangeMap.size());
        }
        int splitRange = range - ((range - lowerKey) >> 1);
        rangeMap.put(splitRange, targetConsumer);
        consumerRange.put(targetConsumer, splitRange);
    }

    private boolean is2Power(int num) {
        if (num < 2) {
            return false;
        }
        return (num & num - 1) == 0;
    }
}

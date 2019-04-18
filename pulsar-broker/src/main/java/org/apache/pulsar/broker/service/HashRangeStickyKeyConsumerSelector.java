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
package org.apache.pulsar.broker.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * This is a consumer selector based hash range.
 *
 * 1.Each consumer serves a fixed range of hash value
 * 2.The whole range of hash value could be covered by all the consumers.
 * 3.Once a consumer is removed, the left consumers could still serve the whole range.
 */
public class HashRangeStickyKeyConsumerSelector implements StickyKeyConsumerSelector {

    public static final int RANGE_SIZE = 2 << 15;

    private final ConcurrentSkipListMap<Integer, Consumer> rangeMap;
    private final Map<Consumer, Integer> consumerRange;

    public HashRangeStickyKeyConsumerSelector() {
        this.rangeMap = new ConcurrentSkipListMap<>();
        this.consumerRange = new HashMap<>();
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) {
        if (rangeMap.size() == 0) {
            rangeMap.put(RANGE_SIZE, consumer);
            consumerRange.put(consumer, RANGE_SIZE);
        } else {
            splitRange(findBiggestRange(), consumer);
        }
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) {
        Integer removeRange = consumerRange.get(consumer);
        if (removeRange != null) {
            if (removeRange == RANGE_SIZE && rangeMap.size() > 1) {
                Consumer lowerConsumer = rangeMap.lowerEntry(removeRange).getValue();
                rangeMap.put(removeRange, lowerConsumer);
                consumerRange.put(lowerConsumer, removeRange);
            } else {
                rangeMap.remove(removeRange);
                consumerRange.remove(consumer);
            }
        }
    }

    @Override
    public Consumer select(String stickyKey) {
        if (rangeMap.size() > 0) {
            int slot = Math.abs(stickyKey.hashCode() % RANGE_SIZE);
            return rangeMap.ceilingEntry(slot).getValue();
        } else {
            return null;
        }
    }

    private int findBiggestRange() {
        int slots = 0;
        int busiestRange = RANGE_SIZE;
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

    private void splitRange(int range, Consumer targetConsumer) {
        Integer lowerKey = rangeMap.lowerKey(range);
        if (lowerKey == null) {
            lowerKey = 0;
        }
        if (range - lowerKey <= 1) {
            throw new RuntimeException("No more range can assigned to new consumer, assigned consumers "
                    + rangeMap.size());
        }
        int splitRange = range - ((range - lowerKey) >> 1);
        rangeMap.put(splitRange, targetConsumer);
        consumerRange.put(targetConsumer, splitRange);
    }
}

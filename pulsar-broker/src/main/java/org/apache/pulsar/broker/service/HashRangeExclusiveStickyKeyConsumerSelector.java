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

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.util.Murmur3_32Hash;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class HashRangeExclusiveStickyKeyConsumerSelector implements StickyKeyConsumerSelector {

    private final int rangeSize;
    private final ConcurrentSkipListMap<Integer, Consumer> rangeMap;

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
    public void addConsumer(Consumer consumer) throws BrokerServiceException.ConsumerAssignException {
        validateKeySharedMeta(consumer);
        for (PulsarApi.IntRange intRange : consumer.getKeySharedMeta().getHashRangesList()) {
            rangeMap.put(intRange.getStart(), consumer);
            rangeMap.put(intRange.getEnd(), consumer);
        }
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        rangeMap.entrySet().removeIf(entry -> entry.getValue().equals(consumer));
    }

    @Override
    public Consumer select(byte[] stickyKey) {
        return select(Murmur3_32Hash.getInstance().makeHash(stickyKey));
    }

    public Consumer select(int hash) {
        if (rangeMap.size() > 0) {
            int slot = hash % rangeSize;
            Map.Entry<Integer, Consumer> ceilingEntry = rangeMap.ceilingEntry(slot);
            Map.Entry<Integer, Consumer> floorEntry = rangeMap.floorEntry(slot);
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

    @Override
    public Consumer selectByIndex(int index) {
        if (rangeMap.size() > 0) {
            Map.Entry<Integer, Consumer> ceilingEntry = rangeMap.ceilingEntry(index);
            Map.Entry<Integer, Consumer> floorEntry = rangeMap.floorEntry(index);
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

    @Override
    public int getRangeSize() {
        return rangeSize;
    }

    private void validateKeySharedMeta(Consumer consumer) throws BrokerServiceException.ConsumerAssignException {
        if (consumer.getKeySharedMeta() == null) {
            throw new BrokerServiceException.ConsumerAssignException("Must specify key shared meta for consumer.");
        }
        List<PulsarApi.IntRange> ranges = consumer.getKeySharedMeta().getHashRangesList();
        if (ranges.isEmpty()) {
            throw new BrokerServiceException.ConsumerAssignException("Ranges for KeyShared policy must not be empty.");
        }
        for (PulsarApi.IntRange intRange : ranges) {

            if (intRange.getStart() > intRange.getEnd()) {
                throw new BrokerServiceException.ConsumerAssignException("Fixed hash range start > end");
            }

            Map.Entry<Integer, Consumer> ceilingEntry = rangeMap.ceilingEntry(intRange.getStart());
            Map.Entry<Integer, Consumer> floorEntry = rangeMap.floorEntry(intRange.getEnd());

            if (floorEntry != null && floorEntry.getKey() >= intRange.getStart()) {
                throw new BrokerServiceException.ConsumerAssignException("Range conflict with consumer " + floorEntry.getValue());
            }

            if (ceilingEntry != null && ceilingEntry.getKey() <= intRange.getEnd()) {
                throw new BrokerServiceException.ConsumerAssignException("Range conflict with consumer " + ceilingEntry.getValue());
            }

            if (ceilingEntry != null && floorEntry != null && ceilingEntry.getValue().equals(floorEntry.getValue())) {
                throw new BrokerServiceException.ConsumerAssignException("Range conflict with consumer " + ceilingEntry.getValue());
            }
        }
    }

    Map<Integer, Consumer> getRangeConsumer() {
        return Collections.unmodifiableMap(rangeMap);
    }

}

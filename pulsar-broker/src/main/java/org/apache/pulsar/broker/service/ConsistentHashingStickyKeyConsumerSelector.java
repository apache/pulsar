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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.impl.StickyKeyConsumerPredicate;
import org.apache.pulsar.common.util.Murmur3_32Hash;

/**
 * This is a consumer selector based fixed hash range.
 *
 * The implementation uses consistent hashing to evenly split, the
 * number of keys assigned to each consumer.
 */
public class ConsistentHashingStickyKeyConsumerSelector implements StickyKeyConsumerSelector {

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // Consistent-Hash ring
    private final NavigableMap<Integer, List<Consumer>> hashRing;

    private final int numberOfPoints;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this.hashRing = new TreeMap<>();
        this.numberOfPoints = numberOfPoints;
    }

    @Override
    public void addConsumer(Consumer consumer) throws ConsumerAssignException {
        rwLock.writeLock().lock();
        Map<Integer, List<Consumer>> hashRingCopy = null;
        try {
            // Insert multiple points on the hash ring for every consumer
            // The points are deterministically added based on the hash of the consumer name
            for (int i = 0; i < numberOfPoints; i++) {
                String key = consumer.consumerName() + i;
                int hash = Murmur3_32Hash.getInstance().makeHash(key.getBytes());
                hashRing.compute(hash, (k, v) -> {
                    if (v == null) {
                        return Lists.newArrayList(consumer);
                    } else {
                        if (!v.contains(consumer)) {
                            v.add(consumer);
                            v.sort(Comparator.comparing(Consumer::consumerName, String::compareTo));
                        }
                        return v;
                    }
                });
            }
            hashRingCopy = new HashMap<>(hashRing);
        } finally {
            rwLock.writeLock().unlock();
        }
        notifyActiveConsumerChanged(hashRingCopy);
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        Map<Integer, List<Consumer>> hashRingCopy = null;
        try {
            // Remove all the points that were added for this consumer
            for (int i = 0; i < numberOfPoints; i++) {
                String key = consumer.consumerName() + i;
                int hash = Murmur3_32Hash.getInstance().makeHash(key.getBytes());
                hashRing.compute(hash, (k, v) -> {
                    if (v == null) {
                        return null;
                    } else {
                        v.removeIf(c -> c.equals(consumer));
                        if (v.isEmpty()) {
                            v = null;
                        }
                        return v;
                    }
                });
            }
            hashRingCopy = new HashMap<>(hashRing);
        } finally {
            rwLock.writeLock().unlock();
        }
        notifyActiveConsumerChanged(hashRingCopy);
    }

    @Override
    public Consumer select(int hash) {
        rwLock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                return null;
            }

            List<Consumer> consumerList;
            Map.Entry<Integer, List<Consumer>> ceilingEntry = hashRing.ceilingEntry(hash);
            if (ceilingEntry != null) {
                consumerList =  ceilingEntry.getValue();
            } else {
                consumerList = hashRing.firstEntry().getValue();
            }

            return consumerList.get(hash % consumerList.size());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Map<Consumer, List<Range>> getConsumerKeyHashRanges() {
        Map<Consumer, List<Range>> result = new LinkedHashMap<>();
        rwLock.readLock().lock();
        try {
            int start = 0;
            for (Map.Entry<Integer, List<Consumer>> entry: hashRing.entrySet()) {
                for (Consumer consumer: entry.getValue()) {
                    result.computeIfAbsent(consumer, key -> new ArrayList<>())
                            .add(Range.of(start, entry.getKey()));
                }
                start = entry.getKey() + 1;
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return result;
    }

    Map<Integer, List<Consumer>> getRangeConsumer() {
        return Collections.unmodifiableMap(hashRing);
    }

    public StickyKeyConsumerPredicate generateSpecialPredicate(final Consumer consumer,
                                                               final Map<Integer, List<Consumer>> hashRing) {
        NavigableMap<Integer, List<String>> cpHashRing = new TreeMap<>();
        for (Map.Entry<Integer, List<Consumer>> entry: hashRing.entrySet()) {
            if (CollectionUtils.isEmpty(entry.getValue())) {
                continue;
            }
            cpHashRing.put(entry.getKey(), entry.getValue()
                    .stream()
                    .map(v -> v == consumer ? StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK
                            : StickyKeyConsumerPredicate.OTHER_CONSUMER_MARK)
                    .collect(Collectors.toList())
            );
        }
        return new StickyKeyConsumerPredicate.Predicate4ConsistentHashingStickyKeyConsumerSelector(cpHashRing);
    }

    private void notifyActiveConsumerChanged(final Map<Integer, List<Consumer>> hashRing) {
        // TODO add configuration for this events in future
        Set<Consumer> consumerSet = new HashSet<>();
        for (List<Consumer> consumerList : hashRing.values()){
            for (Consumer consumer : consumerList){
                if (consumerSet.contains(consumer)){
                    continue;
                }
                consumerSet.add(consumer);
                consumer.notifyActiveConsumerChange(generateSpecialPredicate(consumer, hashRing).encode());
            }
        }
    }
}

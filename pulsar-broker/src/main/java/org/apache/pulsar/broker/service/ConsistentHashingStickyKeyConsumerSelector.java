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

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerAssignException;
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
    private final NavigableMap<Integer, Consumer> hashRing;

    private final int numberOfPoints;

    public ConsistentHashingStickyKeyConsumerSelector(int numberOfPoints) {
        this.hashRing = new TreeMap<>();
        this.numberOfPoints = numberOfPoints;
    }

    @Override
    public void addConsumer(Consumer consumer) throws ConsumerAssignException {
        rwLock.writeLock().lock();
        try {
            // Insert multiple points on the hash ring for every consumer
            // The points are deterministically added based on the hash of the consumer name
            for (int i = 0; i < numberOfPoints; i++) {
                String key = consumer.consumerName() + i;
                int hash = Murmur3_32Hash.getInstance().makeHash(key.getBytes());
                hashRing.put(hash, consumer);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        rwLock.writeLock().lock();
        try {
            // Remove all the points that were added for this consumer
            for (int i = 0; i < numberOfPoints; i++) {
                String key = consumer.consumerName() + i;
                int hash = Murmur3_32Hash.getInstance().makeHash(key.getBytes());
                hashRing.remove(hash, consumer);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Consumer select(byte[] stickyKey) {
        int hash = Murmur3_32Hash.getInstance().makeHash(stickyKey);

        rwLock.readLock().lock();
        try {
            if (hashRing.isEmpty()) {
                return null;
            }

            Map.Entry<Integer, Consumer> ceilingEntry = hashRing.ceilingEntry(hash);
            if (ceilingEntry != null) {
                return ceilingEntry.getValue();
            } else {
                return hashRing.firstEntry().getValue();
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    Map<Integer, Consumer> getRangeConsumer() {
        return Collections.unmodifiableMap(hashRing);
    }
}

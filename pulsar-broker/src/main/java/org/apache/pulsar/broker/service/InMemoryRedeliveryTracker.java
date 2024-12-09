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

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import org.apache.bookkeeper.mledger.Position;

public class InMemoryRedeliveryTracker implements RedeliveryTracker {
    // ledgerId -> entryId -> count
    private Long2ObjectMap<Long2IntMap> trackerCache = new Long2ObjectOpenHashMap<>();
    private final StampedLock rwLock = new StampedLock();

    @Override
    public int incrementAndGetRedeliveryCount(Position position) {
        long stamp = rwLock.writeLock();
        int newCount;
        try {
            Long2IntMap entryMap = trackerCache.computeIfAbsent(position.getLedgerId(),
                    k -> new Long2IntOpenHashMap());
            newCount = entryMap.getOrDefault(position.getEntryId(), 0) + 1;
            entryMap.put(position.getEntryId(), newCount);
        } finally {
            rwLock.unlockWrite(stamp);
        }
        return newCount;
    }

    @Override
    public int getRedeliveryCount(long ledgerId, long entryId) {
        long stamp = rwLock.tryOptimisticRead();
        Long2IntMap entryMap = trackerCache.get(ledgerId);
        int count = entryMap != null ? entryMap.get(entryId) : 0;
        if (!rwLock.validate(stamp)) {
            stamp = rwLock.readLock();
            try {
                entryMap = trackerCache.get(ledgerId);
                count = entryMap != null ? entryMap.get(entryId) : 0;
            } finally {
                rwLock.unlockRead(stamp);
            }
        }
        return count;
    }

    @Override
    public void remove(Position position) {
        long stamp = rwLock.writeLock();
        try {
            Long2IntMap entryMap = trackerCache.get(position.getLedgerId());
            if (entryMap != null) {
                entryMap.remove(position.getEntryId());
                if (entryMap.isEmpty()) {
                    trackerCache.remove(position.getLedgerId());
                }
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void removeBatch(List<Position> positions) {
        if (positions == null) {
            return;
        }
        long stamp = rwLock.writeLock();
        try {
            for (Position position : positions) {
                Long2IntMap entryMap = trackerCache.get(position.getLedgerId());
                if (entryMap != null) {
                    entryMap.remove(position.getEntryId());
                    if (entryMap.isEmpty()) {
                        trackerCache.remove(position.getLedgerId());
                    }
                }
            }
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }

    @Override
    public void clear() {
        long stamp = rwLock.writeLock();
        try {
            trackerCache.clear();
        } finally {
            rwLock.unlockWrite(stamp);
        }
    }
}
